package Connector;
use Moose;
use MooseX::ClassAttribute;

# Base class for Transform plugins
has 'controller' => (is => 'rw', isa => 'Object', required => 1);
has 'user' => (is => 'rw', isa => 'User', required => 1);
has 'results' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'args' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

1;