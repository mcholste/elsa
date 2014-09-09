package Transform::Local;
use Moose;
use Data::Dumper;
use Socket;
extends 'Transform';
our $Name = 'Local';
has 'name' => (is => 'ro', isa => 'Str', required => 1, default => $Name);
has 'known_subnets' => (is => 'ro', isa => 'HashRef', required => 1);
has 'lookup_table' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

sub BUILDARGS {
	my $class = shift;
	my $params = $class->SUPER::BUILDARGS(@_);
	
	$params->{known_subnets} = $params->{conf}->get('transforms/whois/known_subnets');
	
	return $params;
}

sub BUILD {
	my $self = shift;
	
	DATUM_LOOP: foreach my $record ($self->results->all_results){
		$record->{transforms}->{$Name} = {};
		KEY_LOOP: foreach my $key ($self->results->keys($record)){
			next if $key eq 'host';
			my $value = $self->results->value($record, $key);
			if (my @matches = $value =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/g){
				foreach my $ip (@matches){
					if ($self->_is_local($ip)){
						$record->{transforms}->{$Name}->{$key}->{local} = $ip;
						next KEY_LOOP;
					}
				}
			}
		}
	}
	
	$self->log->debug('results: ' . Dumper($self->results));
	
	$self->on_transform->($self->results);
	
	return $self;
}

sub _is_local {
	my $self = shift;
	my $ip = shift;
	my $ip_int = unpack('N*', inet_aton($ip));
	
	foreach my $start (keys %{ $self->known_subnets }){
		my $start_int = unpack('N*', inet_aton($start));
		if ($start_int <= $ip_int and unpack('N*', inet_aton($self->known_subnets->{$start}->{end})) >= $ip_int){
			return 1;
		}
	}
	return 0;
}

 
1;