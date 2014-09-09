package Forwarder;
use Moose;

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => ( is => 'ro', isa => 'Config::JSON', required => 1 );

sub identifiers { [ 'generic' ] };

sub forward {};

__PACKAGE__->meta->make_immutable;

1;