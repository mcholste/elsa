package Transform::Filter;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Filter';
has 'name' => (is => 'ro', isa => 'Str', required => 1, default => $Name);
has 'field' => (is => 'ro', isa => 'Ref', required => 1, default => sub { qr/./ });
has 'regex' => (is => 'ro', required => 1, default => sub { qr/./ });
has 'operator' => (is => 'ro', isa => 'Str');
our $Valid_operators = { map { $_ => 1 } qw(== != > >= < <=) };

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	
	if (scalar @{ $params{args} } > 1){
		# The usual parser may misinterpret commas, we need to reparse our args
		my $reparsed = join(',', @{ $params{args} });
		my $rest;
		($params{field}, $rest) = split(/,/, $reparsed, 2);
		$params{field} = qr/$params{field}/;
		foreach my $op (keys %$Valid_operators){
			if ($rest =~ /\,($op)$/){
				$params{operator} = $1;
				chop($rest); chop($rest);
				last;
			}
		}
		$params{regex} = $rest;
	}
	
	# Catch all in case only regex is given
	unless ($params{regex}){
		$params{field} = qr'.';
		$params{regex} = qr/$params{args}->[0]/i;
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->debug('field: ' . Dumper($self->field));
	$self->log->debug('regex: ' . Dumper($self->regex));
	
	DATUM_LOOP: foreach my $record ($self->results->all_results){
		foreach my $key ($self->results->keys($record)){
			my $value = $self->results->value($record, $key);
			next unless $key =~ $self->field;
			$self->_check($record, $value) and next DATUM_LOOP;
		}
		foreach my $transform (keys %{ $record->{transforms} }){
			next unless ref($record->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $record->{transforms}->{$transform} }){
				if (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					foreach my $key (keys %{ $record->{transforms}->{$transform}->{$transform_field} }){
						next unless "$transform.$transform_field.$key" =~ $self->field;
						if (ref($record->{transforms}->{$transform}->{$transform_field}->{$key}) eq 'ARRAY'){
							foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field}->{$key} }){
								$self->_check($record, $value) and next DATUM_LOOP;
							}
						}
						else {
							$self->_check($record, $record->{transforms}->{$transform}->{$transform_field}->{$key}) and next DATUM_LOOP;
						}
					}
				}
				elsif (ref($record->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY'
					and $transform_field =~ $self->field){
					foreach my $value (@{ $record->{transforms}->{$transform}->{$transform_field} }){
						$self->_check($record, $value) and next DATUM_LOOP;
					}
				}
			}
		}
	}
	
	foreach my $record ($self->results->all_results){
		if (exists $record->{transforms}->{__DELETE__}){
			$self->results->delete_record($record);
		}
	}
	
	$self->log->debug('results: ' . Dumper($self->results));
	
	$self->on_transform->($self->results);
	
	return $self;
}

sub _check {
	my $self = shift;
	my $record = shift;
	my $value = shift;
	
	if ($self->operator){
		if (($self->operator eq '==' and int($value) == int($self->regex))
			or ($self->operator eq '!=' and int($value) != int($self->regex))
			or ($self->operator eq '>' and int($value) > int($self->regex))
			or ($self->operator eq '>=' and int($value) >= int($self->regex))
			or ($self->operator eq '<' and int($value) < int($self->regex))
			or ($self->operator eq '<=' and int($value) <= int($self->regex))){
			$record->{transforms}->{'__DELETE__'} = 1;
			return 1;
		}
	}
	elsif ($value =~ $self->regex){
		$record->{transforms}->{'__DELETE__'} = 1;
		return 1;
	}
	return 0;	
}
 
1;