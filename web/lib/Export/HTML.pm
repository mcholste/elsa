package Export::HTML;
use Moose;
use Data::Dumper;
extends 'Export';
use XML::Writer;
use IO::String;

has 'mime_type' => (is => 'rw', isa => 'Str', required => 1, default => 'text/html');
has 'extension' => (is => 'rw', isa => 'Str', required => 1, default => '.html');

sub BUILD {
	my $self = shift;
	
	my $io = new IO::String;
	my $xw = new XML::Writer(OUTPUT => $io);
	
	$xw->startTag('html');
	$xw->startTag('body');
	$xw->startTag('table');
	
	# Write column headers
	$xw->startTag('tr');
	foreach my $col (@{ $self->columns }){
		$xw->dataElement('th', $col);
	}
	$xw->endTag('tr');
		
	# Write data rows
	foreach my $row (@{ $self->grid }){
		$xw->startTag('tr');
		foreach my $col (@{ $self->columns }){
			$xw->dataElement('td', $row->{$col});
		}
		$xw->endTag('tr');
	}
	
	$xw->endTag('table');
	$xw->endTag('body');
	$xw->endTag('html');
	$xw->end();
		
	$self->results(${ $io->string_ref() });
}

1;