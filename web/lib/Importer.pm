package Importer;
use Moose;

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => ( is => 'ro', isa => 'Config::JSON', required => 1 );
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'lines_to_skip' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'program' => (is => 'rw', isa => 'Str');
has 'timezone' => (is => 'rw', isa => 'Str', required => 1, default => 'local');
has 'host' => (is => 'rw', isa => 'Str');
has 'start' => (is => 'rw', isa => 'Int');
has 'end' => (is => 'rw', isa => 'Int');
has 'year' => (is => 'rw', isa => 'Int');

sub heuristic { return 0 }
sub detect_filename { return 0 }

sub get_header {
	my $self = shift;
	my $id = shift;
	return 'Jan 01 00:00:00 127.0.0.1 ELSA_IMPORT_HEADER: ELSA_IMPORT_ID=' . $id;
}

__PACKAGE__->meta->make_immutable;