package Importer::Unformatted;
use Moose;
extends 'Importer';
use IO::File;
use String::CRC32;
use POSIX qw(strftime);
use Socket;

sub unformatted { return 1 }
# Any file can be a flat file
sub detect_filename { return 1 }
sub heuristic { return 1 }

sub process {
	my $self = shift;
	my $infile_name = shift;
	my $program = shift;
	my $id = shift;
	my $host = shift;
	
	my $infile = new IO::File($infile_name) or die($!);
	my $program_id = crc32($program);
	if ($host){
		$host = unpack('N*', inet_aton($host));
	}
	else {
		$host = unpack('N*', inet_aton('127.0.0.1'));
	}
	
	my $db = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$db = $self->conf->get('syslog_db_name');
	}
	
	my ($query, $sth);
	# Record our program_id to the database if necessary
	$query = 'INSERT IGNORE INTO ' . $db . '.programs (id, program) VALUES(?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($program_id, $program);
	
	#my $date = strftime('%b %d %H:%M:%S', localtime(time()));
	my $date = strftime('%Y-%m-%dT%H:%M:%S.000Z', gmtime(time()));
	
	my $outfile_location = $self->conf->get('buffer_dir') . '/../import';
	my $outfile = new IO::File("> $outfile_location") or die("Cannot open $outfile_location");
	my $counter = 0;
	my $lines_to_skip = $self->lines_to_skip;
	
	# Write header
	#$outfile->print($self->get_header($id) . "\n");
	
	while (<$infile>){
		if ($. <= $lines_to_skip){
			next;
		}
		#$outfile->print("$date $host $program: $_");
		# RFC 5424
		$outfile->print("1 $date $host $program - $id - $_");
		$counter++;
	}
	$outfile->close;
	return $counter;
}

__PACKAGE__->meta->make_immutable;