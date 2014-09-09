package Query::SQL;
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
with 'Fields';

has 'data_db' => (is => 'rw', isa => 'HashRef');

sub BUILD {
	my $self = shift;
	
	return $self;
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

sub _get_table_groups {
	my $self = shift;
	my $cb = shift;
	
	my %groups;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cb->(\%groups);
	});
	
	# Get table schemas
	my ($query, $sth);
	
	my $data_db_name = $self->conf->get('data_db/data_db_name') ? $self->conf->get('data_db/data_db_name') : 'syslog_data';
	
	$query = 'SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=?';
	$self->data_db->{dbh}->query($query, $data_db_name, sub {
		my ($dbh, $rows, $rv) = @_;
		if ($rv and $rows){
			# Format the straight result grid into per-table per-col hashes
			my %schemas;
			foreach my $row (@$rows){
				$schemas{ $row->{TABLE_NAME} } ||= {};
				$schemas{ $row->{TABLE_NAME} }->{ $row->{COLUMN_NAME} } = $row;
			}
			
			foreach my $table_name (keys %schemas){
				my $group_key = '';
				foreach my $col_name (sort { $schemas{$table_name}->{$a}->{ORDINAL_POSITION} <=> $schemas{$table_name}->{$b}->{ORDINAL_POSITION} } keys %{ $schemas{$table_name} }){
					$group_key .= $col_name . ':' . $schemas{$table_name}->{$col_name}->{DATA_TYPE};
				}
				$group_key = crc32($group_key);
				$groups{$group_key} ||= {};
				$groups{$group_key}->{$table_name} = $schemas{$table_name};
			}
				
			$self->meta_info->{tables}->{schemas} = { %schemas };
		}
		else {
			throw(500, 'Unable to get table schemas: ' . $rows);
		}
		$cv->end;
	});
}

sub _get_table_list {
	my $self = shift;
	my $tables_hash = shift;
	
	return [] unless scalar keys %$tables_hash;
	
	my %tables;
	my $start = defined $self->start ? $self->start : 0;
	my $end = defined $self->end ? $self->end : time;
	foreach my $table_hash (@{ $self->meta_info->{tables}->{tables} }){
		my $table_name = $table_hash->{table_name};
		if ($table_name =~ /\./){
			$table_name =~ s/[^\.]+\.//;
		}
		unless (exists $tables_hash->{$table_name}){
			$self->log->error('table not found: ' . $table_name);
		}
		next unless exists $tables_hash->{$table_name};
		
		# Check that the time is right
		if (($table_hash->{start_int} <= $start and $table_hash->{end_int} >= $start)
			or ($table_hash->{start_int} <= $end and $table_hash->{end_int} >= $end)
			or ($table_hash->{start_int} >= $start and $table_hash->{end_int} <= $end)){
			$tables{$table_name} = { start_int => $table_hash->{start_int}, table_name => $table_hash->{table_name} };
		}
	}
	
	# Sort the tables by time
	my $sort_fn;
	if ($self->orderby_dir eq 'DESC'){
		$sort_fn = sub { $tables{$b}->{start_int} <=> $tables{$a}->{start_int} };
	}
	else {
		$sort_fn = sub { $tables{$a}->{start_int} <=> $tables{$b}->{start_int} };
	}
	
	my @ret;
	foreach my $short_table_name (sort $sort_fn keys %tables){
		push @ret, $tables{$short_table_name}->{table_name};
	}
	
	return \@ret;
}


sub _normalize_terms {
	my $self = shift;
	
	# Normalize query terms
	foreach my $boolean (keys %{ $self->terms }){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			my $term_hash = $self->terms->{$boolean}->{$key};
			# Escape any special chars
			$term_hash->{value} =~ s/([^a-zA-Z0-9\.\_\-\@])/\\$1/g;
		}
	}
}

sub estimate_query_time {
	my $self = shift;
	
	my $query_time = 0;
	my $total_rows = 0;
	my $start = defined $self->start ? $self->start : 0;
	my $end = defined $self->end ? $self->end : time;
	
	foreach my $table_hash (@{ $self->meta_info->{tables}->{tables} }){
		# Check that the time is right
		if (($table_hash->{start_int} <= $start and $table_hash->{end_int} >= $start)
			or ($table_hash->{start_int} <= $end and $table_hash->{end_int} >= $end)
			or ($table_hash->{start_int} >= $start and $table_hash->{end_int} <= $end)){
			$total_rows += $table_hash->{records};
		}
		
	}
	
	my $archive_query_rows_per_second = 300_000; # guestimate
	if ($self->conf->get('archive_query_rows_per_second')){
		$archive_query_rows_per_second = $self->conf->get('archive_query_rows_per_second');
	}
	
	$query_time = $total_rows / $archive_query_rows_per_second;
	
	return { estimated_time => $query_time };
}

sub execute {
	my $self = shift;
	my $cb = shift;
	
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
		$self->_get_table_groups(sub {
			my $table_groups = shift;
			foreach my $group_key (keys %$table_groups){
				my $tables = $self->_get_table_list($table_groups->{$group_key});
				
				$self->log->trace('Querying tables: ' . scalar @$tables);
				
				unless (scalar @$tables){
					next;
				}
				
				#my $schema = $table_groups->{$group_key}->{ $tables->[0] };
				my $query = $self->_build_query();
				foreach my $table (@$tables){
					last if $self->limit and $self->results->records_returned >= $self->limit;
					last if $self->check_cancelled;
					if ($self->timeout and (time() - $start) >= ($self->timeout/1000)){
						$self->add_warning(503, 'Query timed out', { timeout => $self->timeout });
						$self->results->is_approximate(1);
						last;
					}
					
					$total++;
					$self->log->debug('$group_key: ' . Dumper($group_key));
					$cv->begin;
					$self->_query($table, $query, sub {
						my $results = shift;
						$counter++;
						if (not $results or $results->{error}){
							$self->log->error('Query error: ' . Dumper($results));
						}
						$cv->end;
					});
					
				}
			}
			
			undef $timeout_watcher;
			$cv->end;
		});
	});
}

sub _build_query {
	my $self = shift;
	my $schema = shift;
	
	my $query = {
		select => $self->_get_select_clause(),
		where => $self->_get_where_clause(),
		groupby => $self->_get_groupby_clause(),
		orderby => $self->_get_orderby_clause(),
	};
	$self->log->debug('query: ' . Dumper($query));
	
	return $query;
}

sub _get_select_clause {
	my $self = shift;
	
	my $clause = '';
	
	if ($self->groupby){
		my $classes = $self->_classes_for_field($self->groupby);
		my @groupby_attr_clause;
		foreach my $class_id (sort keys %$classes){
			my $field = $self->_search_field($self->groupby, $class_id);
			if ($self->_is_meta($self->groupby, $class_id)){
				$field = $self->_attr($self->groupby, $class_id);
			}
			push @groupby_attr_clause, sprintf('class_id=%d, ' . $field, $class_id);
		}
		my $groupby_attr = join(',', map { 'IF(' . $_ } @groupby_attr_clause) . ',id' . join('', map { ')' } @groupby_attr_clause);
		return {
			clause => 'SELECT id, COUNT(*) AS _count, ' . $groupby_attr . ' AS _groupby',
			values => [],
		}
	}
	elsif ($self->orderby){
		my $classes = $self->_classes_for_field($self->orderby);
		my @attr_clause;
		foreach my $class_id (sort keys %$classes){
			my $attr = $self->_attr($self->orderby, $class_id);
			push @attr_clause, sprintf('class_id=%d, ' . $attr, $class_id);
		}
		my $attr = 'IF(' . join(',', @attr_clause) . ',id)';
		return {
			clause => 'SELECT *, ' . $attr . ' AS _orderby',
			values => [],
		}
	}
	return {
		clause => 'SELECT *, timestamp AS _orderby',
		values => [],
	}
}

sub _get_where_clause {
	my $self = shift;
	
	my $clause = scalar keys %{ $self->terms->{and} } ? '1=1' : scalar keys %{ $self->terms->{not} } ? '1=1' : '1=0';
	my @values;
	
	foreach my $boolean (qw(and or not)){
		foreach my $key (sort keys %{ $self->terms->{$boolean} }){
			my $hash = $self->terms->{$boolean}->{$key};
			my $boolean_str = $boolean eq 'and' ? ' AND ' : $boolean eq 'not' ? ' AND NOT ' : ' OR ';
			if ($hash->{field}){
				my @field_clauses;
				foreach my $class_id (keys %{ $self->_classes_for_field($hash->{field}) }){
					my $field = $self->_search_field($hash->{field}, $class_id);
					if ($self->_is_meta($hash->{field})){
						$field = $self->_attr($hash->{field}, $class_id);
					}
					if ($field eq 'class_id'){
						push @field_clauses, '(class_id=?)';
						push @values, $self->_value($hash, $class_id);
					}
					elsif ($field eq 'host_id'){
						push @field_clauses, '(host_id=?)';
						push @values, $self->_value($hash, $class_id);
					}
					elsif ($field eq 'program_id'){
						push @field_clauses, '(program_id=?)';
						push @values, $self->_value($hash, $class_id);
					}
					elsif ($self->_is_int_field($hash->{field}, $class_id)){
						push @field_clauses, '(class_id=? AND ' . $field . '=?)';
						push @values, $class_id, $self->_value($hash, $class_id);
					}
					else {
						push @field_clauses, '(class_id=? AND ' . $field . ' RLIKE ?)';
						push @values, $class_id, $self->_term_to_sql_term($hash->{value}, $hash->{field});
					}
				}
				$clause .= $boolean_str . '(' . join(' OR ', @field_clauses) . ')';
			}
			else {
				if ($hash->{value} =~ /^\((\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\|(\d+)\)$/){
					if ($self->use_sql_regex){
						$clause .= $boolean_str . '(msg RLIKE "' . $self->_term_to_sql_term($1) . '" OR host_id=' . $2 . ')';
					}
					else {
						$clause .= $boolean_str . '(msg LIKE "%' . $1 . '%" OR host_id=' . $2 . ')';
					}
					
				}
				else {
					if ($self->use_sql_regex){
						$clause .= $boolean_str . 'msg RLIKE "' . $self->_term_to_sql_term($hash->{value}) . '"';
					}
					else {
						$clause .= $boolean_str . 'msg LIKE "%' . $hash->{value} . '%"';
					}
				}
			}
		}
	}
	return { clause => $clause, values => [ @values ] };
}

sub _get_groupby_clause {
	my $self = shift;
	
	return $self->groupby ? '_groupby' : '';
}

sub _get_orderby_clause {
	my $self = shift;
	
	return $self->orderby ? '_orderby' : '';
}

sub _query {
	my $self = shift;
	my $table = shift;
	my $query = shift;
	my $cb = shift;
	
	my $cv = AnyEvent->condvar;
	my $ret = { stats => {} };
	$cv->begin( sub{
		$self->log->debug('ret: ' . Dumper($ret));
		$cb->($ret);
	});
	my @values = (@{ $query->{select}->{values} }, @{ $query->{where}->{values} });
	my $query_string = $query->{select}->{clause} . ' FROM ' . $table . ' WHERE ' . $query->{where}->{clause};
	if (defined $self->start){
		$query_string .= ' AND timestamp>=?';
		push @values, $self->start;
	}
	if (defined $self->end){
		$query_string .= ' AND timestamp<=?';
		push @values, $self->end;
	}
	if ($self->groupby){
		$query_string .= ' GROUP BY ' . $query->{groupby} . ' ORDER BY _count ';
	}
	elsif ($self->orderby){
		$query_string .= ' ORDER BY ' . $query->{orderby};
	}
	else {
		$query_string .= ' ORDER BY timestamp';
	}
	if ($self->orderby_dir eq 'DESC'){
		$query_string .= ' DESC';
	}
	else {
		$query_string .= ' ASC';
	}
	$query_string .= ' LIMIT ?,?';
	push @values, ($self->offset, $self->limit);
	
	$self->log->trace('SQL query: ' . $query_string);
	$self->log->trace('SQL query values: ' . join(',', @values));
	
	my $start = time();
	$self->data_db->{dbh}->query($query_string, @values, sub {
		my ($dbh, $rows, $rv) = @_;
		
		my $query_time = (time() - $start);
		$self->log->debug('SQL query finished in ' . $query_time);
		$ret->{stats}->{sql_query} += $query_time;
		
		if (not $rv){
			my $e = 'SQL got error ' .  Dumper($rows);
			$self->log->error($e);
			$self->add_warning(500, $e, { mysql => $self->peer_label });
			$ret = { error => $e };
			$cv->end;
			return;
		}
		$ret->{results} ||= [];
		$self->log->trace('got SQL result: ' . Dumper($rows));
		
		$self->_get_extra_field_values($rows, sub {
			$self->_format_records($rows);
			$cv->end;
		});
	});
}

sub _format_records {
	my $self = shift;
	my $rows = shift;
	
	if ($self->groupby){
		return $self->_format_records_groupby($rows);
	}
	
	my @tmp;
	foreach my $row (@$rows){
		$row->{datasource} = 'Sphinx';
		$row->{_fields} = [
				{ field => 'host', value => inet_ntoa(pack("N*", $row->{host_id})), class => 'any' },
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
		$agg{$node_label} = int(scalar @$rows);
		next;
	}
	foreach my $row (@$rows){
		# Resolve the _groupby col with the mysql col
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
			$key = $self->resolve_value($row->{class_id}, $row->{_groupby}, $self->groupby);
		}
		else {
			# Resolve with the mysql row
			my $field_order = $self->get_field($self->groupby)->{ $row->{class_id} }->{field_order};
			#$self->log->trace('resolving with row ' . Dumper($ret->{results}->{ $sphinx_row->{id} }));
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
			
			my $client_localtime = $unixtime - $self->timezone_diff($unixtime);					
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

sub _get_extra_field_values {
	my $self = shift;
	my $db_rows = shift;
	my $cb = shift;
	
	my %programs;
	foreach my $row (@$db_rows){
		$programs{ $row->{program_id} } = $row->{program_id};
	}
	if (not scalar keys %programs){
		$cb->($db_rows);
		return;
	}
	
	my $query;
	$query = 'SELECT id, program FROM programs WHERE id IN (' . join(',', map { '?' } keys %programs) . ')';
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cb->($db_rows);
	});
	
	$self->data_db->{dbh}->query($query, (sort keys %programs), sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error getting extra field values ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cv->end;
			return;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get extra field value rows though we had values: ' . Dumper(\%programs)); 
		}
		else {
			$self->log->trace('got extra field value db rows: ' . (scalar @$rows));
			foreach my $row (@$rows){
				$programs{ $row->{id} } = $row->{program};
			}
			foreach my $row (@$db_rows){
				$row->{program} = $programs{ $row->{program_id} };
			}
		}
		$cv->end;
	});
}

1;