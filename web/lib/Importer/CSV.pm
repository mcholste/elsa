package Importer::CSV;
use Moose;
extends 'Importer';
use IO::File;
use Socket;
use Text::CSV;
use String::CRC32 qw(crc32);

has 'separator' => (is => 'rw', isa => 'Str', required => 1, default => ',');
has 'quote_char' => (is => 'rw', isa => 'Str', required => 1, default => '\'');
has 'class' => (is => 'rw', isa => 'Str', required => 1);

sub csv { return 100 }
sub detect_filename {
	my $self = shift;
	my $filename = shift;
	if ($filename =~ /\.csv$/){
		$self->log->trace('Found match for filename ' . $filename);
		return 5;
	}
	return 0;
}
sub heuristic {
	my $self = shift;
	open(FH, shift) or die($!);
	my $first_line = <FH>;
	close(FH);
	if ($first_line =~ /(?:[^,]+,){3,}/){
		$self->log->trace('Heuristically detected a possible match for CSV');
		# While we're at it, let's heuristically find our separator
		# count "
		my @m = $first_line =~ /(")/;
		my $double_matches = scalar @m;
		# count '
		@m = $first_line =~ /(')/;
		my $single_matches = scalar @m;
		# See which has more
		if ($double_matches){
			if ($single_matches){
				if ($double_matches > $single_matches){
					$self->quote_char('"');
				}
			}
			else {
				$self->quote_char('"');
			}
		}
		return 2;
	}
	return 0;
}

sub process {
	my $self = shift;
	my $infile_name = shift;
	my $program = shift;
	my $id = shift;
	my $host = shift;
	
	if ($host){
		$host = unpack('N*', inet_aton($host));
	}
	else {
		$host = unpack('N*', inet_aton('127.0.0.1'));
	}
	
	my $infile = new IO::File($infile_name) or die($!);
	
	my $csv = new Text::CSV({
		sep_char => $self->separator,
		quote_char => $self->quote_char,
		blank_is_undef => 1,
		empty_is_undef => 1,
	});
	# Create a new file which has the right host_id to represent the import id
	my $outfile_name = $self->conf->get('buffer_dir') . '/' . time();
	my $outfile = new IO::File('> ' . $outfile_name);
	my $program_id = crc32($self->program);
	
	my $db = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$db = $self->conf->get('syslog_db_name');
	}
	
	my ($query, $sth);
	# Record our program_id to the database if necessary
	$query = 'INSERT IGNORE INTO ' . $db . '.programs (id, program) VALUES(?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($program_id, $self->program);
	
	# Get columns from first line
	my $first_line = <$infile>;
	chomp($first_line);
	my @cols = split(/,/, $first_line);
	
	$query = 'SELECT id FROM ' . $db . '.classes WHERE class=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->class);
	my $row = $sth->fetchrow_hashref;
	die('Invalid class ' . $self->db . ' given') unless $row;
	my $class_id = $row->{id};
	
	$query = 'SELECT field_order FROM ' . $db . '.fields_classes_map t1 JOIN ' . $db . '.fields t2 ON (t1.field_id=t2.id) ' .
		'WHERE t2.field=? AND t1.class_id=?';
	$sth = $self->db->prepare($query);
	my @field_orders;
	for (my $i = 0; $i < @cols; $i++){
		if ($cols[$i] eq 'timestamp'){
			$field_orders[$i] = 1;
		}
		else {
			$sth->execute($cols[$i], $class_id);
			my $row = $sth->fetchrow_hashref;
			unless ($row){
				die('Unable to find a field_order for column ' . $cols[$i] . ' and class ' . $self->class);
			}
			$field_orders[$i] = $row->{field_order} + 1; # add one to account for the auto-inc value we need to add
		}
	}
	
	
	my $counter = 0;
	my $default_time = time();
	my ($earliest, $latest);
	while (my $line = $csv->getline($infile)){
		my @ordered_line = (0, $default_time, $host, $program_id, $class_id, join(',', @$line),
			undef, undef, undef, undef, undef, undef, undef, undef, undef, undef, undef, undef);
		for (my $i = 0; $i < @$line; $i++){
			# Escape backslashes
			$line->[$i] =~ s/\\/\\\\/g;
			# Put this value in the correct field order for the schema
			$ordered_line[ $field_orders[$i] ] = $line->[$i];
		}
		$earliest ||= $ordered_line[1];
		$latest = $ordered_line[1];
		
		$outfile->print(join("\t", @ordered_line) . "\n");
		$counter++;
	}
	$outfile->close;
	
	$query = 'INSERT INTO ' . $db . '.buffers (filename, start, end, import_id) VALUES (?,?,?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($outfile_name, $earliest, $latest, $id);
	
	return $counter;
}

__PACKAGE__->meta->make_immutable;