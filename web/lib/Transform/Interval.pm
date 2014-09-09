package Transform::Interval;
use Moose;
use Data::Dumper;
extends 'Transform';
our $Name = 'Interval';

has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->trace('data: ' . Dumper($self->data));
	
	# Handle single result
	if ((scalar @{ $self->data}) == 1){
		$self->data->[0]->{transforms}->{ $self->name } = { timestamp => { interval => 0 } };
		return $self;
	}
	
	for (my $i = 0; $i < scalar @{ $self->data }; $i++){
		my $datum = $self->data->[$i];
		$datum->{transforms}->{ $self->name } = { timestamp => {} };
		my $interval = 0;
		if (not $i and $self->data->[$i+1]){ #first
			$interval = $self->data->[$i+1]->{timestamp} - $datum->{timestamp};
		}
		else {
			$interval = $datum->{timestamp} - $self->data->[$i-1]->{timestamp};
		}
		$datum->{transforms}->{ $self->name }->{timestamp} = { interval => $interval };
	}
	
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

 
1;