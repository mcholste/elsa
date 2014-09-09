package Export::CSV;
use Moose;
use Data::Dumper;
extends 'Export';

has 'extension' => (is => 'rw', isa => 'Str', required => 1, default => '.csv');

sub BUILD {
	my $self = shift;
	
	# Write column headers
	my $text = join(",", @{ $self->columns }) . "\n";
	
	# Write data rows
	foreach my $row (@{ $self->grid }){
		my @vals;
		foreach my $col (@{ $self->columns }){
			push @vals, $row->{$col};
		}
		$text .= join(",", @vals) . "\n";
	}
	
	$self->results($text);
}

1;