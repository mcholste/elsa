package Datasource;
use Moose;
use Log::Log4perl;

# Base class for Datasource plugins
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'log' => (is => 'rw', isa => 'Object', required => 1);
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'args' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

sub query {
	my $self = shift;
	my $q = shift;
	my $cb = shift;

	$self->_is_authorized($q) or die('Unauthorized');
	$self->_query($q, $cb);
	
	return $q;	
}

sub _is_authorized {
	my $self = shift;
	my $q = shift;
	
	return 1;
}

1;