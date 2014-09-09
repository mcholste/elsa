package Warnings;
use Moose::Role;
use Data::Dumper;

has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_warnings' => 'count', 'clear_warnings' => 'clear', 'all_warnings' => 'elements' });

sub add_warning {
	my $self = shift;
	my $code = shift;
	my $errstr = shift;
	my $data = shift;
	
	if ($code and ref($code) and ref($code) eq 'Ouch'){
		push @{ $self->warnings }, $code;
	}
	else {
		$self->log->warn($code . ': ' . $errstr . ', ' . Dumper($data));
		push @{ $self->warnings }, new Ouch($code, $errstr, $data);
	}
}

sub errors {
	my $self = shift;
	my @errors;
	foreach my $e (@{ $self->warnings }){
		push @errors, $e if $e->code >= 500;
	}
	return \@errors;
}

sub has_errors {
	my $self = shift;
	my $count = 0;
	foreach my $e (@{ $self->warnings }){
		$count++ if $e->code >= 500;
	}
	return $count;
}

1;