package Forwarder::Copy;
use Moose;
use Data::Dumper;
use File::Copy;
extends 'Forwarder';

has 'dir' => (is => 'rw', isa => 'Str', required => 1);
sub identifiers { [ 'dir' ] };

sub forward {
	my $self = shift;
	my $args = shift;
	
	$self->log->trace('Copying file ' . $args->{file});
	move($args->{file}, $self->dir . '/') or ($self->log->error('Error copying ' . $args->{file} 
		. ' to dir ' . $self->dir . ': ' . $!) and return 0);
		
	return 1;					
}

__PACKAGE__->meta->make_immutable;

1;