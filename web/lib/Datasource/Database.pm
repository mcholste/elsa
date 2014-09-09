package Datasource::Database;
use Moose;
use Moose::Meta::Class;
use Data::Dumper;
use CHI;
use DBI;
use JSON;
use URL::Encode qw(url_encode);
use Time::HiRes qw(time);
use Search::QueryParser::SQL;
use Date::Manip;
use Socket;
use Try::Tiny;
use Ouch qw(:trytiny);
use AnyEvent;
extends 'Datasource';
with 'Fields';

our $Name = 'Database';
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'dsn' => (is => 'rw', isa => 'Str', required => 1);
has 'username' => (is => 'rw', isa => 'Str', required => 1);
has 'password' => (is => 'rw', isa => 'Str', required => 1);
has 'query_template' => (is => 'rw', isa => 'Str', required => 1);
has 'fields' => (is => 'rw', isa => 'ArrayRef', required => 1);
has 'parser' => (is => 'rw', isa => 'Object');
has 'db' => (is => 'rw', isa => 'Object');
has 'timestamp_column' => (is => 'rw', isa => 'Str');
has 'timestamp_is_int' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'query_object' => (is => 'rw', isa => 'Object');
has 'gmt_offset' => (is => 'rw', isa => 'Int');
has 'window_offset' => (is => 'rw', isa => 'Int');

our %Numeric_types = ( int => 1, ip_int => 1, float => 1 );
our %Mixed_types = ( proto => 1 );

sub BUILD {
	my $self = shift;
	
	# LongReadLen will allow MS-SQL to read in n number bytes before giving a truncation error
	$self->db(DBI->connect($self->dsn, $self->username, $self->password, { RaiseError => 1, LongReadLen => 20_000 }));
	my ($query, $sth);
	
	$self->query_template =~ /FROM\s+([\w\_]+)/;
	my %cols;
	foreach my $row (@{ $self->fields }){
		if (not $row->{type}){
			if ($self->dsn =~ /dbi:Pg/){
				$row->{fuzzy_op} = 'ILIKE';
				$row->{fuzzy_not_op} = 'NOT ILIKE';
			}
			else {
				$row->{fuzzy_op} = 'LIKE';
				$row->{fuzzy_not_op} = 'NOT LIKE';
			}
		}
		elsif ($row->{type} and $Numeric_types{ $row->{type} }){
			$row->{fuzzy_op} = '=';
			$row->{fuzzy_not_op} = '!=';
		}
		else {
			$row->{fuzzy_not_op} = '<=';
		}
		
			
		if ($row->{type} and $Numeric_types{ $row->{type} }){
			$row->{callback} = sub {
				my ($col, $op, $val) = @_;
				$self->log->debug("adding to query: $col $op " . unpack('N*', inet_aton($val)));
				return "$col $op " . unpack('N*', inet_aton($val));
			};
		}
		elsif (not $Mixed_types{ $row->{name} }){
			$row->{callback} = sub {
				my ($col, $op, $val) = @_;
				if ($op eq '='){
					return "$col $row->{fuzzy_op} '%$val%'";
				}
				else {
					return "$col $row->{fuzzy_not_op} '%$val%'";
				}
			};
		}
		
		if ($row->{alias}){
			if ($row->{alias} eq 'timestamp'){
				$self->timestamp_column($row->{name});
			}
			elsif ($row->{alias} eq 'timestamp_int'){
				$self->timestamp_column($row->{name});
				$self->timestamp_is_int(1);
			}
			$cols{ $row->{alias} } = $row;
		}
		
		$cols{ $row->{name} } = $row;
	}
			
	foreach my $field (keys %$Fields::Reserved_fields){
		if ($field eq 'datasource'){
			$cols{$field} = { name => $field, callback => sub { '' } };
		}
		else {
		 	$cols{$field} = { name => $field, callback => sub { 
		 		my ($col, $op, $val) = @_;
		 		$self->log->debug('set_directive: ' . join(',', $col, $op, $val));
		 		$self->query_object->set_directive($col, $val, $op);
		 		return undef;
		 		}
	 		};
		}
	 	#$cols{$field} = { name => $field, callback => sub { '' } };
	}
	
	$self->log->debug('cols ' . Dumper(\%cols));
	$self->parser(Search::QueryParser::SQL->new(columns => \%cols));
	
	return $self;
}

sub _query {
	my $self = shift;
	my $q = shift;
	my $cb = shift;
	$self->query_object($q);
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { $cb->() });
	
	my ($query, $sth);
	
	my $query_string = $q->query_string;
	$query_string =~ s/\|.*//;
	
	$self->log->debug('query: ' . $query_string);
	
	my ($where, $placeholders);
	try {
		($where, $placeholders) = @{ $self->parser->parse($query_string)->dbi };
		#$self->log->debug('query now: ' . Dumper($self->query_object));
	}
	catch {
		my ($err) = split(/\n/, $_, 0);
		$err =~ s/ at \/.+\.pm line \d+\.//;
		throw(400, $err, { query_string => $query_string });
	};
	
	#$where =~ s/(?:(?:AND|OR|NOT)\s*)?1=1//g; # clean up dummy values
	$where =~ s/(?:AND|OR|NOT)\s*$//; # clean up any trailing booleans
	$self->log->debug('where: ' . Dumper($where));
	
	my @select;
	my $groupby = '';
	my $time_select_conversions = {
		iso => {
			year => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/(86400*365) AS unsigned)',
			month => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/(86400*30) AS unsigned)',
			week => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/(86400*7) AS unsigned)',
			day => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/86400 AS unsigned)',
			hour => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/3600 AS unsigned)',
			minute => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/60 AS unsigned)',
			seconds => 'UNIX_TIMESTAMP(' . $self->timestamp_column . ')',
		},
		int => {
			year => 'CAST(' . $self->timestamp_column . '/(86400*365) AS unsigned)',
			month => 'CAST(' . $self->timestamp_column . '/(86400*30) AS unsigned)',
			week => 'CAST(' . $self->timestamp_column . '/(86400*7) AS unsigned)',
			day => 'CAST(' . $self->timestamp_column . '/86400 AS unsigned)',
			hour => 'CAST(' . $self->timestamp_column . '/3600 AS unsigned)',
			minute => 'CAST(' . $self->timestamp_column . '/60 AS unsigned)',
			seconds => $self->timestamp_column,
		}
	};
	
	if ($q->groupby){
		# Check to see if there is a numeric count field
		my $count_field;
		foreach my $field (@{ $self->fields }){
			if ($field->{alias} and $field->{alias} eq 'count'){
				$count_field = $field->{name};
			}
		}
		
		if ($self->timestamp_is_int and $time_select_conversions->{int}->{ $q->groupby }){
			if ($count_field){
				push @select, 'SUM(' . $count_field . ') AS `_count`', $time_select_conversions->{int}->{ $q->groupby } . ' AS `_groupby`';
			}
			else {
				push @select, 'COUNT(*) AS `_count`', $time_select_conversions->{int}->{ $q->groupby } . ' AS `_groupby`';
			}
			$groupby = 'GROUP BY _groupby';
		}
		elsif ($time_select_conversions->{iso}->{ $q->groupby }){
			if ($count_field){
				push @select, 'SUM(' . $count_field . ') AS `_count`', $time_select_conversions->{iso}->{ $q->groupby } . ' AS `_groupby`';
			}
			else {
				push @select, 'COUNT(*) AS `_count`', $time_select_conversions->{iso}->{ $q->groupby } . ' AS `_groupby`';
			}
			$groupby = 'GROUP BY _groupby';
		}
		elsif ($q->groupby eq 'node'){
			#TODO Need to break this query into subqueries if grouped by node
			throw(501, 'not supported', { directive => 'groupby' });
		}
		else {
			foreach my $field (@{ $self->fields }){
				if ($field->{alias} eq $q->groupby or $field->{name} eq $q->groupby){
					if ($count_field){
						push @select, 'SUM(' . $count_field . ') AS _count', $field->{name} . ' AS _groupby';
					}
					else {
						if ($field->{type} eq 'ip_int'){
							push @select, 'COUNT(*) AS _count', 'INET_NTOA(' . $field->{name} . ')' . ' AS _groupby';
						}
						else {
							push @select, 'COUNT(*) AS _count', $field->{name} . ' AS _groupby';
						}
					}
					$groupby = 'GROUP BY ' . $q->groupby;
					last;
				}
			}
		}	
		unless ($groupby){
			throw(400, 'Invalid groupby ' . $groupby, { directive => 'groupby' });
		}
	}
	
	my ($start, $end, $start_int, $end_int);
	if (defined $self->gmt_offset){
		$start_int = $q->start + $self->gmt_offset;
		$start = epoch2iso($start_int, 1);
		$end_int = $q->end + $self->gmt_offset;
		$end = epoch2iso($end_int, 1);
	}
	else {
		$start_int = $q->start;
		$start = epoch2iso($q->start);
		$end_int = $q->end;
		$end = epoch2iso($q->end);
	}
	
	if ($q->groupby){
		if ($time_select_conversions->{iso}->{ $q->groupby }){
			foreach my $row (@{ $self->fields }){
				if ($row->{alias}){
					if ($row->{alias} eq 'timestamp'){
						if ($where and $where ne ' '){
							$where = '(' . $where . ') AND ' . $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
						}
						else {
							$where = $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
						}
						push @$placeholders, $start, $end;
						last;
					}
					elsif ($row->{alias} eq 'timestamp_int'){
						if ($where and $where ne ' '){
							$where = '(' . $where . ') AND ' . $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
						}
						else {
							$where = $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
						}
						push @$placeholders, $start_int, $end_int;
						last;
					}
				}
			}
		}
	}
	else {
		foreach my $row (@{ $self->fields }){
			if ($row->{alias}){
				if ($row->{type} eq 'ip_int'){
					push @select, 'INET_NTOA(' . $row->{name} . ')' . ' AS ' . $row->{alias};
				}
				else {
					push @select, $row->{name} . ' AS ' . $row->{alias};
				}
				if ($row->{alias} eq 'timestamp'){
					if ($where and $where ne ' '){
						$where = '(' . $where . ') AND ' . $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
					}
					else {
						$where = $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
					}
					push @$placeholders, $start, $end;
				}
				elsif ($row->{alias} eq 'timestamp_int'){
					if ($where and $where ne ' '){
						$where = '(' . $where . ') AND ' . $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
					}
					else {
						$where = $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
					}
					push @$placeholders, $start_int, $end_int;
				}
			}
			else {
				push @select, $row->{name};
			}
		}
	}

	my $orderby;
	if ($q->groupby){
		if ($time_select_conversions->{iso}->{ $q->groupby }){
			$orderby = '_groupby ASC';
		}
		else {
			$orderby = '_count DESC';
		}
	}
	else {
		$orderby = '1';
	}
	
	# Sane default for where
	$where = '1=1' unless $where;
	
	$query = sprintf($self->query_template, join(', ', @select), $where, $groupby, $orderby, $q->offset, $q->limit);
	$self->log->debug('query: ' . $query);
	$self->log->debug('placeholders: ' . Dumper($placeholders));
	$sth = $self->db->prepare($query);
	$sth->execute(@$placeholders);
	
	my $overall_start = time();
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		$self->log->debug('row: ' . Dumper($row));
		push @rows, $row;
	}
	if ($q->groupby){
		my %results;
		my $total_records = 0;
		my $records_returned = 0;
		my @tmp;
		
		if (exists $Fields::Time_values->{ $q->groupby }){
			# Sort these in ascending label order
			my $increment = $Fields::Time_values->{ $q->groupby };
			my $use_gmt = $increment >= 86400 ? 1 : 0;
			my %agg; 
			foreach my $row (@rows){
				my $unixtime = $row->{_groupby};
				my $value = $unixtime * $increment;
									
				$self->log->trace('$value: ' . epoch2iso($value, 1) . ', increment: ' . $increment . 
					', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($value)));
				$row->{intval} = $value;
				$agg{ $row->{intval} } += $row->{_count};
			}
			
			foreach my $key (sort { $a <=> $b } keys %agg){
				push @tmp, { 
					intval => $key, 
					_groupby => epoch2iso($key, $use_gmt), 
					_count => $agg{$key}
				};
			}	
			
			# Fill in zeroes for missing data so the graph looks right
			my @zero_filled;
			
			$self->log->trace('using increment ' . $increment . ' for time value ' . $q->groupby);
			OUTER: for (my $i = 0; $i < @tmp; $i++){
				push @zero_filled, $tmp[$i];
				if (exists $tmp[$i+1]){
					for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
						#$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
						push @zero_filled, { 
							_groupby => epoch2iso($j, $use_gmt),
							intval => $j,
							_count => 0
						};
						last OUTER if scalar @zero_filled > $q->limit;
					}
				}
			}
			$results{$q->groupby} = [ @zero_filled ];
		}
		elsif (UnixDate($rows[0]->{_groupby}, '%s') > UnixDate('2000-01-01 00:00:00', '%s') 
			and UnixDate($rows[0]->{_groupby}, '%s') < UnixDate('2020-01-01 00:00:00', '%s')){
			# Sort these in ascending label order
			my $increment = 86400 * 30;
			my $use_gmt = $increment >= 86400 ? 1 : 0;
			my %agg; 
			foreach my $row (@rows){
				my $unixtime = UnixDate($row->{_groupby}, '%s');
				my $value = $unixtime - ($unixtime % $increment);
									
				$self->log->trace('key: ' . epoch2iso($value, $use_gmt) . ', tv: ' . $increment . 
					', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($value)));
				$row->{intval} = $value;
				$agg{ $row->{intval} } += $row->{_count};
			}
			
			foreach my $key (sort { $a <=> $b } keys %agg){
				push @tmp, { 
					intval => $key, 
					_groupby => epoch2iso($key, 1), #$self->resolve_value(0, $key, $groupby), 
					_count => $agg{$key}
				};
			}	
			
			# Fill in zeroes for missing data so the graph looks right
			my @zero_filled;
			
			$self->log->trace('using increment ' . $increment . ' for time value ' . $q->groupby);
			OUTER: for (my $i = 0; $i < @tmp; $i++){
				push @zero_filled, $tmp[$i];
				if (exists $tmp[$i+1]){
					$self->log->debug('$tmp[$i]->{intval} ' . $tmp[$i]->{intval});
					$self->log->debug('$tmp[$i+1]->{intval} ' . $tmp[$i+1]->{intval});
					for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
						$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
						push @zero_filled, { 
							_groupby => epoch2iso($j, 1),
							intval => $j,
							_count => 0
						};
						last OUTER if scalar @zero_filled > $q->limit;
					}
				}
			}
			$results{$q->groupby} = [ @zero_filled ];
		}
		else { 
			# Sort these in descending value order
			foreach my $row (sort { $b->{_count} <=> $a->{_count} } @rows){
				$total_records += $row->{_count};
				$row->{intval} = $row->{_count};
				push @tmp, $row;
				last if scalar @tmp > $q->limit;
			}
			$results{$q->groupby} = [ @tmp ];
		}
		$records_returned += scalar @tmp;
		
		if (ref($q->results) eq 'Results::Groupby'){
			$q->results->add_results(\%results);
		}
		else {
			$q->results(Results::Groupby->new(conf => $self->conf, results => \%results, total_records => $total_records));
		}
	}
	else {
		foreach my $row (@rows){
			my $timestamp = $row->{timestamp_int};
			if ($row->{timestamp}){
				if ($row->{timestamp} =~ /^\d+$/){
					# Already native
					$timestamp = $row->{timestamp};
				}
				else {
					# Convert
					$timestamp = UnixDate(ParseDate($row->{timestamp}), '%s');
				}
			}
			else {
				# Already native via timestamp_int
			}
			my $ret = { timestamp => $timestamp, class => 'NONE', host => $q->peer_label, 'program' => 'NA', datasource => $self->name };
			$ret->{_fields} = [
				{ field => 'host', value => $q->peer_label, class => 'any' },
				{ field => 'program', value => 'NA', class => 'any' },
				{ field => 'class', value => 'NONE', class => 'any' },
			];
			my @msg;
			foreach my $key (sort keys %$row){
				push @msg, $key . '=' . $row->{$key};
				push @{ $ret->{_fields} }, { field => $key, value => $row->{$key}, class => 'NONE' };
			}
			$ret->{msg} = join(' ', @msg);
			$q->results->add_result($ret);
			last if $q->limit and scalar $q->results->total_records >= $q->limit;
		}
	}
			
	$q->time_taken(time() - $overall_start);
	
	$self->log->debug('completed query in ' . $q->time_taken . ' with ' . $q->results->total_records . ' rows');
	$self->log->debug('results: ' . Dumper($q->results));
	
	$cv->end;
}

 
1;
