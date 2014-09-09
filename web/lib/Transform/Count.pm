package Transform::Count;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Count';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	$params{groupby} = $params{args}->[0];
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	my $sums = {};
	foreach my $record ($self->results->all_results){
		foreach my $transform (keys %{ $record->{transforms} }){
			next unless ref($record->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $record->{transforms}->{$transform} }){
				if (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					if (exists $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }){
						if (ref($record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }) eq 'ARRAY'){
							foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }){
								$sums->{ $value }++;
							}
						}
						else {
							$sums->{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }++;
						}
					}
				}
				elsif (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY' 
					and $transform_field eq $self->groupby){
					foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field} }){
						$sums->{ $value }++;
					}
				}
			}
		}
		if (defined $self->results->value($record, $self->groupby)){
			if ($self->results->value($record, $self->groupby) =~ /^\d+$/){
				$sums->{ $self->groupby }++;
			}
			else {
				$sums->{ $self->results->value($record, $self->groupby) }++;
			}
		}
	}
	$self->log->debug('sums: ' . Dumper($sums));
	my $ret = [];
	foreach my $key (keys %$sums){
		push @$ret, { _groupby => $key, intval => $sums->{$key}, _count => $sums->{$key} };
	}
	
	$self->on_transform->(Results::Groupby->new(results => { $self->groupby => [ sort { $b->{intval} <=> $a->{intval} } @$ret ] }));
	
	return $self;
}

 
1;