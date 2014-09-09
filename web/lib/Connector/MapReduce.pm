package Connector::MapReduce;
use Moose;
use Data::Dumper;
extends 'Connector';

# This is a demo for how to write a map/reduce connector in ELSA.  It doesn't do anything other than save the result.

our $Timeout = 10;
our $DefaultTimeOffset = 120;
our $Description = 'Run map/reduce';
sub description { return $Description }
sub admin_required { return 1 }

has 'query_schedule_id' => (is => 'rw', isa => 'Num', required => 1);
has 'qid' => (is => 'rw', isa => 'Num', required => 1);
has 'records_returned' => (is => 'rw', isa => 'Num', required => 1);
has 'query_string' => (is => 'rw', isa => 'Str', required => 1);
has 'query_meta_params' => (is => 'rw', isa => 'HashRef', required => 1);

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
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	# Perform "reduce" function here
	foreach my $record (@{ $self->results->{results} }){
		$self->api->log->debug('timestamp: ' . $record->{timestamp});
	}
	
	# Save the results
	$self->api->save_results({
		#meta_info => { groupby => $self->query_meta_params->{groupby} },
		meta_info => {},
		qid => $self->qid, 
		results => $self->results, 
		comments => 'Map/Reduce for ' . $self->query_schedule_id,
	});
	
	return 1;
}


1