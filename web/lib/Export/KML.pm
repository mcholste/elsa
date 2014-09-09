package Export::KML;
use Moose;
use Data::Dumper;
extends 'Export';
use XML::Writer;
use IO::String;

has 'mime_type' => (is => 'rw', isa => 'Str', required => 1, default => 'application/vnd.google-earth.kml+xml');
has 'extension' => (is => 'rw', isa => 'Str', required => 1, default => '.kml');

sub BUILD {
	my $self = shift;
	
	my $io = new IO::String;
	my $xw = new XML::Writer(OUTPUT => $io);
	
	$xw->xmlDecl("UTF-8");
	$xw->startTag('kml', 'xmlns' => 'http://www.opengis.net/kml/2.2');
	$xw->startTag('Document');
	$xw->dataElement('open', '1');
	$xw->startTag('Folder');
	$xw->dataElement('name', 'Places');
	$xw->dataElement('open', '1');
	$xw->dataElement('title', 'ELSA');
	
	# Write data rows
	foreach my $row (@{ $self->grid }){
		if ($row->{'ip.latitude'}){
			 $xw->startTag('Placemark');
			 $xw->dataElement('name', $row->{city});
			 $xw->dataElement('description', $row->{msg});
			 $xw->startTag('Point');
			 $xw->dataElement('coordinates', join(',', $row->{'ip.longitude'}, $row->{'ip.latitude'}));
			 $xw->endTag('Point');
			 $xw->endTag('Placemark');
		}
		elsif ($row->{'srcip.latitude'}){
			foreach my $dir (qw(srcip dstip)){
				$xw->startTag('Placemark');
				$xw->dataElement('name', $row->{$dir . '.city'} ? $row->{$dir . '.city'} : $row->{$dir . '.cc'});
				$xw->dataElement('description', $row->{msg});
				$xw->startTag('LookAt');
				$xw->dataElement('longitude', $row->{$dir . '.longitude'});
				$xw->dataElement('latitude', $row->{$dir . '.latitude'});
				$xw->dataElement('altitude', '10000');
				$xw->dataElement('tilt', '0');
				$xw->dataElement('heading', '0');
				$xw->endTag('LookAt');
				 
				 $xw->startTag('MultiGeometry');
				 
				 $xw->startTag('LineString');
				 $xw->dataElement('tessellate', '1');
				 $xw->dataElement('coordinates', join(',', $row->{'srcip.longitude'}, $row->{'srcip.latitude'}) . "\n" . 
				 	join(',', $row->{'dstip.longitude'}, $row->{'dstip.latitude'}));
				 $xw->endTag('LineString');
				 
				 $xw->startTag('Point');
				 $xw->dataElement('coordinates', join(',', $row->{$dir . '.longitude'}, $row->{$dir . '.latitude'}));
				 $xw->endTag('Point');
				 
				 $xw->endTag('MultiGeometry');
				 $xw->endTag('Placemark');
			}
		}
	}
	
	$xw->endTag('Folder');
	$xw->endTag('Document');
	$xw->endTag('kml');
		
	$self->results(${ $io->string_ref() });
}

1;