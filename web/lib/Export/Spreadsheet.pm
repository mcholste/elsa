package Export::Spreadsheet;
use Moose;
use Data::Dumper;
extends 'Export';
use Spreadsheet::WriteExcel;
use IO::String;

has 'mime_type' => (is => 'rw', isa => 'Str', required => 1, default => 'application/excel');
has 'extension' => (is => 'rw', isa => 'Str', required => 1, default => '.xls');

sub BUILD {
	my $self = shift;
	
	# Create an in-memory filehandle for our result
	my $io = new IO::String();
	
	# Create a new Excel workbook
	my $workbook = Spreadsheet::WriteExcel->new($io);

	# Add a worksheet
	my $worksheet = $workbook->add_worksheet();
	
	my @cols = @{ $self->columns };
	
	# Write column headers
	for (my $i = 0; $i <= $#cols; $i++){
		$worksheet->write(0, $i, $cols[$i]);
	}
	
	# Write data rows
	my $row_counter = 1;
	foreach my $row (@{ $self->grid }){
		for (my $i = 0; $i <= $#cols; $i++){
			$worksheet->write($row_counter, $i, $row->{ $cols[$i] });
		}
		$row_counter++;	
	}
	
	$workbook->close();
	
	$self->results(${ $io->string_ref() });
}

1;