package Connector::ForEach;
use Moose;
use Data::Dumper;
extends 'Connector';

# This will take the results of one query and run a subsequent query for each found

our $DefaultTimeOffset = 120;
our $Description = 'For Each Result Run Subquery';
sub description { return $Description }
sub admin_required { return 1 }

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if (ref($params{results}) eq 'HASH' and $params{results}->{results}){
		$params{records_returned} = $params{results}->{recordsReturned};
		foreach my $attr (qw(qid query_string query_meta_params)){
			if (exists $params{results}->{$attr}){
				$params{$attr} = $params{results}->{$attr};
			}
		}
	}
	
	# If we were given an AoA, deref
	if (ref($params{args}->[0]) and ref($params{args}->[0]) eq 'ARRAY' and scalar @{ $params{args} } == 1){
		$params{args} = $params{args}->[0];
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	my $query_template = $self->args->[0];
		
	my $time_offset = $DefaultTimeOffset;
	if (scalar @{ $self->args } > 1){
		$time_offset = int($self->args->[1]);
	}
	
	# Find the field we're going to extract
	$query_template =~ /\$\{([\w\.]+)\}/;
	my $field = $1;
	$query_template =~ s/%/%%/g; #escape any percent signs in the query
	$query_template =~ s/\$\{([\w\.]+)\}/%s/g; # swap the template text with a %s for sprintf'ing later
	die('No ${<field name>} given to use as the search template term') unless $field;
	
	my $results;
	my $last_qid;
	my @flat;
	if (ref($self->results->{results}) eq 'HASH'){
		# Process a given groupby result set
		my @groupbys;
		foreach my $groupby (keys %{ $self->results->{results} }){
			#push @flat, @{ $self->results->{results}->{$groupby} };
			foreach my $record (@{ $self->results->{results}->{$groupby} }){
				unless ($record->{_groupby}){
					$self->api->log->warn('unable to find substitute ' . $field . ' in record ' . Dumper($record));
					next;
				}
				my $query = sprintf($query_template, $record->{_groupby});
				my $q = $self->api->query({query_string => $query, user => $self->user, system => 1});
				$last_qid = $q->qid;
				#$self->api->log->debug('results: ' . Dumper($q->results->all_results));
				if ($q->has_groupby){
					$results ||= new Results::Groupby();
					$results->add_results({ $record->{_groupby} => $q->results->all_results });
					push @groupbys, $record->{_groupby};
				}
				else {
					$results ||= new Results();
					$results->add_results([ $q->results->all_results ]);
				}
			}
		}
		# Save the results
		my $to_save = {
			qid => $last_qid, 
			results => $results->TO_JSON->{results},
			num_results => $results->total_records,
			comments => 'ForEach run against ' . (scalar @groupbys) . ' template values',
		};
		if (scalar @groupbys){
			$to_save->{groupby} = [ @groupbys ];
		}
		$self->api->save_results($to_save);
	}
	else {
		# We were given a straight array of records
		$results = new Results();
		foreach my $record (@{ $self->results->{results} }){
			my $substitute;
			foreach my $key (keys %$record){
				if ($key eq $field){
					$substitute = $record->{$key};
					last;
				}
			}
			# Not a native field, try the _fields array
			if (not $substitute and exists $record->{_fields}){
				foreach my $field_hash (@{ $record->{_fields} }){
					if ($field eq $field_hash->{field}){
						$substitute = $field_hash->{value};
						last;
					}
				}
			}
			unless ($substitute){
				$self->api->log->warn('unable to find substitute ' . $field . ' in record ' . Dumper($record));
				next;
			}
			
			my $query = sprintf($query_template, $substitute);
			my $meta_params = {
				start => $record->{timestamp} - $time_offset,
				end => $record->{timestamp} + $time_offset,
			};
			my $q = $self->api->query({query_string => $query, query_meta_params => $meta_params, user => $self->user, system => 1});
			$last_qid = $q->qid;
			#$self->api->log->debug('results: ' . Dumper($q->results->all_results));
			$results->add_results([ $q->results->all_results ]);
		}
		# Save the results
		$self->api->save_results({
			qid => $last_qid,
			num_results => $results->total_records,
			results => $results->TO_JSON->{results},
			comments => 'ForEach run against ' . (scalar (@{ $self->results->{results} })) . ' template values',
		});
	}
	
	return 1;
}


1