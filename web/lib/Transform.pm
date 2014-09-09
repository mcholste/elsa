package Transform;
use Moose;
use Log::Log4perl;

# Base class for Transform plugins
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'log' => (is => 'rw', isa => 'Object', required => 1);
has 'results' => (is => 'rw', isa => 'Object', required => 1, default => sub { [] });
has 'user' => (is => 'rw', isa => 'User');
has 'on_transform' => (is => 'rw', isa => 'CodeRef', required => 1);
has 'on_error' => (is => 'rw', isa => 'CodeRef', required => 1);
# A transform may be a "meta" tranform which refers to other transforms
has 'transforms' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'groupby' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'args' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

1;