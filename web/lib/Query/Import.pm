package Query::Import;
use Moose;
use Data::Dumper;
use AnyEvent;
use Try::Tiny;
use Ouch qw(:trytiny);
use Socket;
use String::CRC32;
use Sys::Hostname::FQDN;
use Net::DNS;
use Time::HiRes qw(time);

extends 'Query';

our $Max_limit = 10_000; # safety to not run out of memory if asked to sift through a large import

has 'data_db' => (is => 'rw', isa => 'HashRef');

sub execute {
	my $self = shift;
	my $cb = shift;
	
	if ($self->parser->has_import_search_terms){
		$self->_find_import_ranges();
		if (not $self->has_id_ranges){
			$self->log->trace('Import terms eliminate all results');
			$cb->();
			return;
		}
	}
	else {
		throw(400, 'No import terms found');
	}
	
	my $start = time();
	my $counter = 0;
	my $total = 0;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		if (not $self->has_errors){
			$total and $self->results->percentage_complete(100 * $counter / $total);
			if ($self->results->total_records > $self->results->total_docs){
				$self->results->total_docs($self->results->total_records);
			}
		}
		$cb->();
	});
	
	my $timeout_watcher;
	if ($self->timeout){
		$timeout_watcher = AnyEvent->timer(after => ($self->timeout/1000), cb => sub {
			$self->add_warning(503, 'Query timed out', { timeout => $self->timeout });
			$cv->send;
			undef $timeout_watcher;
			return;
		});
	}	
	
	# Get the tables based on our search
	$self->_get_db(sub {
		$self->_get_rows(sub {
			undef $timeout_watcher;
			$self->time_taken(time - $start);
			$cv->end;
		});
	});
}

sub _get_db {
	my $self = shift;
	my $cb = shift;
	my $conf = $self->conf->get('data_db');
	
	my $start = time();
	my $db_name = 'syslog';
	if ($conf->{db}){
		$db_name = $conf->{db};
	}
	
	my $mysql_port = 3306;
	if ($conf->{port}){
		$mysql_port = $conf->{port};
	}
			
	my $ret = {};
	eval {
		$ret = { db => $db_name };
		$ret->{dbh} = SyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:database=' . $db_name . ';port=' . $mysql_port,  
			$conf->{username}, 
			$conf->{password}, 
			{
				mysql_connect_timeout => $self->db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
			}
		]);
	};
	if ($@){
		$self->add_warning(502, $@, { mysql => $self->peer_label });
		$cb->(0);
	}		
	
	$self->log->trace('All connected in ' . (time() - $start) . ' seconds');
	$self->data_db($ret);
	
	$cb->(1);
}



sub _get_rows {
	my $self = shift;
	my $cb = shift;
	
	my $start = time();
	my $total_records = 0;
	my $counter = 0;
	my %tables;
	foreach my $range ($self->all_id_ranges){
		$total_records += $range->{values}->[1] - $range->{values}->[0];
		last if $counter >= $Max_limit;
		# Find what tables we need to query to resolve rows
		
		ROW_LOOP: for (my $id = $range->{values}->[0]; $id <= $range->{values}->[1]; $id++){
			foreach my $table_hash (@{ $self->meta_info->{tables}->{tables} }){
				last ROW_LOOP if $counter >= $Max_limit;
				next unless $table_hash->{table_type} eq 'import';
				if ($table_hash->{min_id} <= $id and $id <= $table_hash->{max_id}){
					$tables{ $table_hash->{table_name} } ||= [];
					push @{ $tables{ $table_hash->{table_name} } }, $id;
					$counter++;
					next ROW_LOOP;
				}
			}
		}
	}
		
	if (not scalar keys %tables){
		$self->add_warning(500, 'Data not yet indexed, try again shortly.', { mysql => $self->peer_label });
		$self->log->error('No tables found for result. tables: ' . Dumper($self->meta_info->{tables}));
		$cb->();
		return;
	}			
		
	# Go get the actual rows from the dbh
	my @table_queries;
	my @table_query_values;
	foreach my $table (sort keys %tables){
		my $placeholders = join(',', map { '?' } @{ $tables{$table} });
		push @table_query_values, @{ $tables{$table} };
		my $table_query;
		
		if ($self->groupby){
			my $groupby_select_clause = '';
		
			my $field_hashes = $self->get_field($self->groupby);
			unless (scalar keys %$field_hashes){
				throw(400, 'Field ' . $self->groupby . ' not a valid groupby value', { term => $self->groupby });
			}
			foreach my $class_id (keys %$field_hashes){
				$groupby_select_clause .= 'IF(class_id=' . $class_id . ',' . $Fields::Field_order_to_field->{ $field_hashes->{$class_id}->{field_order} } . ',';
			}
			$groupby_select_clause .= 0;
			foreach my $class_id (keys %$field_hashes){
				$groupby_select_clause .= ')';
			}
			$groupby_select_clause .= ' AS _groupby';
			
			$table_query = sprintf("SELECT COUNT(*) AS _count, $groupby_select_clause, id,\n" .
				"timestamp, INET_NTOA(host_id) AS host, program_id, class_id, msg,\n" .
				"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
				"FROM %1\$s\n" .
				'WHERE id IN (' . $placeholders . ') ', $table);
		}
		else {
			$table_query = sprintf("SELECT id,\n" .
				"timestamp, INET_NTOA(host_id) AS host, program_id, class_id, msg,\n" .
				"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
				"FROM %1\$s\n" .
				'WHERE id IN (' . $placeholders . ') ', $table);
		}
		
		if (defined $self->start){
			$table_query .= ' AND timestamp>=?';
			push @table_query_values, $self->start;
		}
		if (defined $self->end){
			$table_query .= ' AND timestamp<=?';
			push @table_query_values, $self->end;
		}
		
		if ($self->groupby){
			$table_query .= "\nGROUP BY _groupby";
		}
		
		push @table_queries, $table_query;
	}
	
	if (not @table_queries){
		$self->add_warning(500, 'No tables found for import ids', { mysql => $self->peer_label });
		$self->log->error('No tables found for import ids: ' . Dumper(\%tables));
		$cb->();
		return;
	}

	my $table_query = join(' UNION ', @table_queries) . ' ' .
	($self->orderby ? 'ORDER BY ' . $self->orderby . ' ' . $self->orderby_dir . ' ' : ' ') .
	'LIMIT ?,?';
	push @table_query_values, $self->offset, $self->limit;
	$self->log->trace('table query: ' . $table_query 
		. ', placeholders: ' . join(',', @table_query_values));
	
	$self->data_db->{dbh}->query($table_query, @table_query_values, sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cb->();
			return;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get rows though we had results! tables: ' . Dumper(\%tables));
			$cb->();
			return; 
		}
		$self->log->trace('got db rows: ' . (scalar @$rows));
		
		if ($self->groupby){
			$self->stats->{mysql_query} += (time() - $start);
			$self->_get_extra_field_values($rows, sub {
				my $rows = shift;
				$self->_format_records_groupby($rows);
				$cb->();
			});
		}
		else {
			foreach my $row (@$rows){
				$row->{node} = $self->peer_label ? $self->peer_label : '127.0.0.1';
				$row->{node_id} = unpack('N*', inet_aton($row->{node}));
				if ($self->orderby){
					$row->{_orderby} = $row->{ $self->orderby };
				}
				else {
					$row->{_orderby} = $row->{timestamp};
				}
				# Copy import info into the row
				foreach my $range ($self->all_id_ranges){
					if ($range->{values}->[0] <= $row->{id} and $row->{id} <= $range->{values}->[1]){
						foreach my $import_col (qw(name description)){
							$row->{'import_' . $import_col} = $range->{import_info}->{$import_col};
						}
						$row->{'import_date'} = $range->{import_info}->{imported};
						last;
					}
					$self->log->error('no range for id ' . $row->{id} . ', ranges: ' . Dumper($self->id_ranges));
				}
			}
		
			$self->stats->{mysql_query} += (time() - $start);
			$self->_get_extra_field_values($rows, sub {
				my $rows = shift;
				$self->_format_records($rows);
				$cb->();
			});
		}
	});	
}

sub _get_extra_field_values {
	my $self = shift;
	my $rows = shift;
	my $cb = shift;
	
	my %programs;
	foreach my $row (@$rows){
		$programs{ $row->{program_id} } = $row->{program_id};
	}
	if (not scalar keys %programs){
		$cb->($rows);
		return;
	}
	
	my $query;
	$query = 'SELECT id, program FROM programs WHERE id IN (' . join(',', map { '?' } keys %programs) . ')';
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cb->($rows);
	});
	$self->data_db->{dbh}->query($query, (sort keys %programs), sub { 
		my ($dbh, $program_rows, $rv) = @_;
		if (not $rv or not ref($program_rows) or ref($program_rows) ne 'ARRAY'){
			my $errstr = 'got error getting extra field values ' . $program_rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cv->end;
			return;
		}
		elsif (not scalar @$program_rows){
			$self->log->error('Did not get extra field value rows though we had values: ' . Dumper(\%programs)); 
		}
		else {
			$self->log->trace('got extra field value db rows: ' . (scalar @$program_rows));
			foreach my $row (@$program_rows){
				$programs{ $row->{id} } = $row->{program};
			}
			foreach my $row (@$rows){
				$row->{program} = $programs{ $row->{program_id} };
			}
		}
		$cv->end;
	});
}

sub _format_records {
	my $self = shift;
	my $rows = shift;
	
	my @tmp;
	foreach my $row (@$rows){
		$row->{datasource} = 'Import';
		$row->{_fields} = [
				{ field => 'host', value => $row->{host}, class => 'any' },
				{ field => 'program', value => $row->{program}, class => 'any' },
				{ field => 'class', value => $self->meta_info->{classes_by_id}->{ $row->{class_id} }, class => 'any' },
			];
		my $is_import = 0;
		foreach my $import_col (@{ $Fields::Import_fields }){
			if (exists $row->{$import_col}){
				$is_import++;
				push @{ $row->{_fields} }, { field => $import_col, value => $row->{$import_col}, class => 'any' };
			}
		}
		if ($is_import){					
			# Add node
			push @{ $row->{_fields} }, { field => 'node', value => $row->{node}, class => 'any' };
		}
		# Resolve column names for fields
		foreach my $col (qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5)){
			my $value = delete $row->{$col};
			# Swap the generic name with the specific field name for this class
			my $field = $self->meta_info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
			if (defined $value and $field){
				# See if we need to apply a conversion
				$value = $self->resolve_value($row->{class_id}, $value, $col);
				push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->meta_info->{classes_by_id}->{ $row->{class_id} } };
			}
		}
		push @tmp, $row;
	}
	
	
	# Now that we've got our results, order by our given order by
	if ($self->orderby_dir eq 'DESC'){
		foreach my $row (sort { $b->{_orderby} <=> $a->{_orderby} } @tmp){
			$self->results->add_result($row);
			last if $self->results->records_returned >= $self->limit;
		}
	}
	else {
		foreach my $row (sort { $a->{_orderby} <=> $b->{_orderby} } @tmp){
			$self->log->debug('adding row: ' . Dumper($row));
			$self->results->add_result($row);
			last if $self->results->records_returned >= $self->limit;
		}
	}
	
	$self->results->total_docs($self->results->total_docs + scalar @$rows);
}

sub _format_records_groupby {
	my $self = shift;
	my $rows = shift;
	
	my %agg;
	my $total_records = 0;
	
	# One-off for grouping by node
	if ($self->groupby eq 'node'){
		my $node_label = $self->peer_label ? $self->peer_label : '127.0.0.1';
		$agg{$node_label} = scalar @$rows;
		next;
	}
	foreach my $row (@$rows){
		my $key;
		if (exists $Fields::Time_values->{ $self->groupby }){
			# We will resolve later
			$key = $row->{'_groupby'};
		}
		elsif ($self->groupby eq 'program'){
			$key = $row->{program};
		}
		elsif ($self->groupby eq 'class'){
			$key = $self->meta_info->{classes_by_id}->{ $row->{class_id} };
		}
		elsif (defined $Fields::Field_to_order->{ $self->groupby }){
			# Resolve normally
			$key = $self->resolve_value($row->{class_id}, $row->{'_groupby'}, $self->groupby);
		}
		else {
			# Resolve with the mysql row
			my $field_order = $self->get_field($self->groupby)->{ $row->{class_id} }->{field_order};
			$key = $row->{ $Fields::Field_order_to_field->{$field_order} };
			$key = $self->resolve_value($row->{class_id}, $key, $Fields::Field_order_to_field->{$field_order});
			$self->log->trace('field_order: ' . $field_order . ' key ' . $key);
		}
		$agg{ $key } += $row->{'_count'};	
	}
	
	if (exists $Fields::Time_values->{ $self->groupby }){
		# Sort these in ascending label order
		my @tmp;
		my $increment = $Fields::Time_values->{ $self->groupby };
		my $use_gmt = $increment >= 86400 ? 1 : 0;
		foreach my $key (sort { $a <=> $b } keys %agg){
			$total_records += $agg{$key};
			my $unixtime = $key * $increment;
			
			my $client_localtime = $unixtime - $self->parser->timezone_diff($unixtime);					
			$self->log->trace('key: ' . $key . ', tv: ' . $increment . 
				', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($client_localtime)));
			push @tmp, { 
				intval => $unixtime, 
				'_groupby' => epoch2iso($client_localtime, $use_gmt),
				'_count' => $agg{$key}
			};
		}
		
		# Fill in zeroes for missing data so the graph looks right
		my @zero_filled;
		
		$self->log->trace('using increment ' . $increment . ' for time value ' . $self->groupby);
		OUTER: for (my $i = 0; $i < @tmp; $i++){
			push @zero_filled, $tmp[$i];
			if (exists $tmp[$i+1]){
				for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
					push @zero_filled, { 
						'_groupby' => epoch2iso($j, $use_gmt),
						intval => $j,
						'_count' => 0
					};
					last OUTER if scalar @zero_filled >= $self->limit;
				}
			}
		}
		$self->results->add_results({ $self->groupby => [ @zero_filled ] });
	}
	else { 
		# Sort these in descending value order
		my @tmp;
		foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
			$total_records += $agg{$key};
			push @tmp, { intval => $agg{$key}, '_groupby' => $key, '_count' => $agg{$key} };
			last if scalar @tmp >= $self->limit;
		}
		$self->results->add_results({ $self->groupby => [ @tmp ] });
		$self->log->debug('@tmp: ' . Dumper(\@tmp));
	}
}

1;