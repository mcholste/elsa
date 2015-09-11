package Transform::Cluster;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Cluster';
has 'name' => (is => 'ro', isa => 'Str', required => 1, default => $Name);
has 'field' => (is => 'ro', isa => 'Str', required => 1, default => '');
has 'clusters' => (is => 'ro', isa => 'Num', required => 1, default => 10);

our $Tolerance = .001;

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	$params{field} = $params{args}->[0];
	if (scalar @{ $params{args} } > 1){
		$params{clusters} = $params{args}->[1];
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->debug('field: ' . Dumper($self->field));
	$self->log->debug('clusers: ' . Dumper($self->clusters));
	
	my @data;
	foreach my $record ($self->results->all_results){
		my $value = $self->results->value($record, $self->field);
		next unless defined($value);
		push @data, $value;
	}

	# initialize by choosing random points the data
	my @center = @data[ map {rand @data} 1..$self->clusters ];

	my $diff;
	do {
	    $diff = 0;

	    # Assign points to nearest center
	    my @cluster;
	    foreach my $point (@data) {
	        my $closest = 0;
	        my $dist = abs $point - $center[ $closest ];
	        for my $idx (1..$#center) {
	            if (abs $point - $center[ $idx ] < $dist) {
	                $dist = abs $point - $center[ $idx ];
	                $closest = $idx;
	            }
	        }
	        push @cluster, [$point, $closest];
	    }

	    # compute new centers
	    foreach my $center_idx (0..$#center) {
	        my @members = grep {$_->[1] == $center_idx} @cluster;
	        my $sum = 0;
	        foreach my $member (@members) {
	            $sum += $member->[0];
	        }
	        my $new_center = @members ? $sum / @members : $center[ $center_idx ];
	        $diff += abs $center[ $center_idx ] - $new_center;
	        $center[ $center_idx ] = $new_center;
	    }

	} while ($diff > $Tolerance);

	foreach my $record ($self->results->all_results){
		my $value = $self->results->value($record, $self->field);
		my $cluster = $self->_get_cluster(\@center, $value);
		$record->{transforms} ||= {};
		$record->{transforms}->{$Name} = {};
		$record->{transforms}->{$Name}->{ $self->field } = { cluster => $cluster };
	}
	
	$self->log->debug('results: ' . Dumper($self->results));
	
	$self->on_transform->($self->results);
	
	return $self;
}

sub _get_cluster {
	my $self = shift;
	my $center = shift;
	my $value = shift;
    my $closest = 0;
    my @c = @$center;
    my $dist = abs $value - $c[ $closest ];
    for my $idx (1..$#c) {
        if (abs $value - $c[ $idx ] < $dist) {
            $dist = abs $value - $c[ $idx ];
            $closest = $idx;
        }
    }
    return $closest + 1;
}
 
1;