package Export::PDF;
use Moose;
use Data::Dumper;
extends 'Export';
use PDF::API2::Simple;
use IO::String;

has 'mime_type' => (is => 'rw', isa => 'Str', required => 1, default => 'application/pdf');
has 'extension' => (is => 'rw', isa => 'Str', required => 1, default => '.pdf');

sub BUILD {
	my $self = shift;
	
	# Create an in-memory filehandle for our result
	my $io = new IO::String();
	
	# Create a new PDF
	my $pdf = PDF::API2::Simple->new( file => $io );

	$pdf->add_font('Verdana');

	# Add a page
	$pdf->add_page();
	
	# Write column headers
	my $text = join("\t", @{ $self->columns });
	$pdf->text($text, autoflow => 'on');

	# Write data rows
	foreach my $row (@{ $self->grid }){
		$text = '';
		for (my $i = 0; $i < (scalar @{ $self->columns } ); $i++){
			$text .= $row->{ $self->columns->[$i] } . "\t";
		}
		$pdf->text($text, autoflow => 'on');	
	}
	$self->results($pdf->as_string);
}

1;