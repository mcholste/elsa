package Transform::Sum;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Sum';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	$params{groupby} = $params{args}->[0];
	$params{log}->trace('records: ' . Dumper($params{results}));
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->trace('records: ' . Dumper($self->results->results));
	
	my $sums = {};
	foreach my $record ($self->results->all_results){
		$record->{transforms} ||= {};
		$record->{transforms}->{$Name} = {};
		foreach my $transform (keys %{ $record->{transforms} }){
			next unless ref($record->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $record->{transforms}->{$transform} }){
				if (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					if (exists $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }){
						if (ref($record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }) eq 'ARRAY'){
							foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }){
								if ($value =~ /^\d+$/){
									$sums->{ $value } += $value;
								}
								elsif ($record->{count}){
									$sums->{$value} += $record->{count};
								}
								else {
									$sums->{ $value }++;
								}
							}
						}
						else {
							if ($record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } =~ /^\d+$/){
								$sums->{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } } += 
									$record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby };
							}
							elsif ($record->{count}){
								$sums->{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } } += $record->{count};
							}
							else {
								$sums->{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }++;
							}
						}
					}
				}
				elsif (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY' 
					and $transform_field eq $self->groupby){
					foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field} }){
						if ($value =~ /^\d+$/){
							$sums->{ $value } += $value;
						}
						elsif ($record->{count}){
							$sums->{$value} += $record->{count};
						}
						else {
							$sums->{ $value }++;
						}
					}
				}
			}
		}
		if (my $value = $self->results->value($record, $self->groupby)){
			if ($value =~ /^\d+$/){
				$sums->{ $self->groupby } += $value;
			}
			elsif ($self->results->value($record, 'count')){
				$sums->{ $self->groupby } += $self->results->value($record, 'count');
			}
			else {
				$sums->{$value}++;
			}
		}
	}
	my $ret = [];
	foreach my $key (keys %$sums){
		push @$ret, { _groupby => $key, intval => $sums->{$key}, _count => $sums->{$key} };
	}
	
	# Sort
	#$ret = [ sort { $b->{intval} <=> $a->{intval} } @$ret ];
	
	$self->on_transform->(Results::Groupby->new(results => { $self->groupby => [ sort { $b->{intval} <=> $a->{intval} } @$ret ] }));
	
	return $self;
}

 
1;