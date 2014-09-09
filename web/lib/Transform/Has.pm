package Transform::Has;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Has';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'count' => (is => 'ro', isa => 'Num', required => 1, default => 0);
has 'operator' => (is => 'ro', isa => 'Str', required => 1, default => '>=');

our $Valid_operators = {
	'>' => 1,
	'<' => 1,
	'>=' => 1,
	'<=' => 1,
	'=' => 1,
	'==' => 1,
};

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	if (defined $params{args}->[0]){
		$params{count} = sprintf('%d', $params{args}->[0]);
	}
	if ($params{args}->[1]){
		$params{operator} = $Valid_operators->{ $params{args}->[1] } ? $params{args}->[1] : '>';
	}
	$params{groupby} = (sort $params{results}->all_groupbys)[0];
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->trace('results: ' . Dumper($self->results->results));
	
	DATUM_LOOP: foreach my $record ($self->results->all_results){
		foreach my $transform (keys %{ $record->{transforms} }){
			next unless ref($record->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $record->{transforms}->{$transform} }){
				if (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					if (exists $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }){
						if (ref($record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }) eq 'ARRAY'){
							foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }){
								my $test = int($value) . ' ' . $self->operator . ' ' . $self->count;
								if (eval($test)){
									#$self->log->trace('passed value ' . $value);
									$record->{transforms}->{$Name} = '__KEEP__';
									next DATUM_LOOP;
								}
							}
						}
						else {
							if ($record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } =~ /^\d+$/){
								my $test = int($record->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }) . ' ' . $self->operator . ' ' . $self->count;
								if (eval($test)){
									#$self->log->trace('passed value ' . $value);
									$record->{transforms}->{$Name} = '__KEEP__';
									next DATUM_LOOP;
								}
							}
						}
					}
				}
				elsif (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY' 
					and $transform_field eq $self->groupby){
					foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field} }){
						my $test = int($value) . ' ' . $self->operator . ' ' . $self->count;
						if (eval($test)){
							#$self->log->trace('passed value ' . $value);
							$record->{transforms}->{$Name} = '__KEEP__';
							next DATUM_LOOP;
						}
					}
				}
			}
		}
		if (exists $record->{ $self->groupby } ){
			my $test = int($record->{count}) . ' ' . $self->operator . ' ' . $self->count;
			#$self->log->trace('test: ' . $test);
			if (eval($test)){
				#$self->log->trace('passed value ' . $value);
				$record->{transforms}->{$Name} = '__KEEP__';
				next DATUM_LOOP;
			}
		}
		elsif (exists $record->{_count}){
			my $test = int($record->{_count}) . ' ' . $self->operator . ' ' . $self->count;
			if (eval($test)){
				#$self->log->trace('passed value ' . $value);
				$record->{transforms}->{$Name} = '__KEEP__';
				next DATUM_LOOP;
			}
		}
	}
	
	my $ret = [];
	
	foreach my $record ($self->results->all_results){
		if (exists $record->{transforms}->{$Name}){
			delete $record->{transforms}->{$Name}; # no need to clutter our final results
			if (exists $record->{_groupby}){
				push @$ret, $record;
			}
			else {
				push @$ret, { 
					_groupby => $record->{ $self->groupby }, 
					intval => $record->{count}, 
					_count => $record->{count},
					count => $record->{count},
					$self->groupby => $record->{ $self->groupby },
				};
			}
		}
	}
	
	# Sort
	$ret = [ sort { $b->{intval} <=> $a->{intval} } @$ret ];
	$self->results->results({ $self->groupby => $ret });
		
	$self->log->debug('final data: ' . Dumper($self->results->results));
	
	$self->on_transform->($self->results);
	
	return $self;
}

 
1;