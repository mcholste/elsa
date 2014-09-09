package Importer::Syslog;
use Moose;
extends 'Importer';
use IO::File;
use DateTime;
use DateTime::Format::Strptime;

sub local_syslog { return 1 }
sub heuristic {
	my $self = shift;
	open(FH, shift) or die($!);
	my $first_line = <FH>;
	close(FH);
	if ($first_line =~ /^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\ \d{2}\:\d{2}\:\d{2}\ /){
		$self->log->trace('Heuristically detected a possible match for syslog');
		return 1;
	}
	return 0;
}
sub detect_filename {
	my $self = shift;
	my $filename = shift;
	my $possible_filenames = { map { $_ => 1 } qw(syslog messages kern.log mail.log mail.err pm-powersave.log 
		pycentral.log ufw.log user.log Xorg.0.log cron.log daemon.log debug dpkg.log error 
		alternatives.log apport.log auth.log boot.log) };
	if ($possible_filenames->{$filename}){
		$self->log->trace('Found match for filename ' . $filename);
		return 1;
	}
	return 0;
}

sub process {
	my $self = shift;
	my $infile_name = shift;
	my $program = shift;
	my $id = shift;
	
	my $infile = new IO::File($infile_name) or die($!);
	my $outfile_location = $self->conf->get('buffer_dir') . '/../import';
	my $outfile = new IO::File("> $outfile_location") or die("Cannot open $outfile_location");
	my $counter = 0;
	my $lines_to_skip = $self->lines_to_skip;
	my $timezone = DateTime::TimeZone->new( name => $self->timezone )->name;
	my $parser = DateTime::Format::Strptime->new(pattern => '%b %d %T %Y', time_zone => $timezone);
	my $start = 2**32;
	my $end = 0;
	my @localtime = localtime;
	my $year = $self->year ? $self->year : $localtime[5] + 1900;
	
	# Write header
	#$outfile->print($self->get_header($id) . "\n");
	
	while (<$infile>){
		if ($. <= $lines_to_skip){
			next;
		}
		$_ =~ /^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\ \d{2}\:\d{2}\:\d{2})\ (\S+)\ ([^\:]+): ([^\n]+)/;
		my $dt = $parser->parse_datetime("$1 $year") or next;
		my ($host, $program, $msg) = ($2, $3, $4);
		my $date = $dt->strftime('%Y-%m-%dT%H:%M:%S.000Z');
		if ($dt->epoch < $start){
			$start = $dt->epoch;
		}
		if ($dt->epoch > $end){
			$end = $dt->epoch;
		}
		#$outfile->print($_);
		$outfile->print("1 $date $host $program - $id - $msg\n");
		$counter++;
	}
	$self->start($start);
	$self->end($end);
	return $counter;
}

__PACKAGE__->meta->make_immutable;