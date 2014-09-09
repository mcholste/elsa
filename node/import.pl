#!/usr/bin/perl
use strict;
use Data::Dumper;
use Getopt::Std;
use DateTime;
use DateTime::Format::Strptime;
use File::Temp;
use Config::JSON;
use String::CRC32;
use Time::HiRes qw(time);
use IO::File;
use POSIX qw(strftime);
use DBI;
use FindBin;
use Socket;

# Include the directory this script is in
use lib $FindBin::Bin;

use Reader;
use Indexer;

my %Opts;
my $Infile_name = pop(@ARGV);
die('Non-existent or no infile given ' . usage()) unless -f $Infile_name;
getopts('c:t:f:s:n:p:d:C:', \%Opts);
print "Working on $Infile_name\n";
my $Name = $Opts{n} ? $Opts{n} : $Infile_name;
my $Description = $Opts{d} ? $Opts{d} : 'manual import';
my $Infile = new IO::File($Infile_name);
my $Lines_to_skip = defined $Opts{s} ? int($Opts{s}) : 0;
my $Format = defined $Opts{f} ? $Opts{f} : 'local_syslog';
my $Conf_file = $Opts{c} ? $Opts{c} : '/etc/elsa_node.conf';
my $Config_json = Config::JSON->new( $Conf_file );
my $Conf = $Config_json->{config}; # native hash is 10x faster than using Config::JSON->get()

# Setup logger
my $logdir = $Conf->{logdir};
my $debug_level = $Conf->{debug_level};
my $l4pconf = qq(
	log4perl.category.ELSA       = $debug_level, File
	log4perl.appender.File			 = Log::Log4perl::Appender::File
	log4perl.appender.File.filename  = $logdir/node.log
	log4perl.appender.File.syswrite = 1
	log4perl.appender.File.recreate = 1
	log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
	log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
	log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
	log4perl.filter.ScreenLevel.LevelMin  = $debug_level
	log4perl.filter.ScreenLevel.LevelMax  = ERROR
	log4perl.filter.ScreenLevel.AcceptOnMatch = true
	log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
	log4perl.appender.Screen.Filter = ScreenLevel 
	log4perl.appender.Screen.stderr  = 1
	log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
	log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
);
Log::Log4perl::init( \$l4pconf ) or die("Unable to init logger\n");
my $Log = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");

my $Dbh = DBI->connect(($Conf->{database}->{dsn} or 'dbi:mysql:database=syslog;'), 
	$Conf->{database}->{username}, 
	$Conf->{database}->{password}, 
	{
		RaiseError => 1,
		mysql_local_infile => 1,
	}) 
	or die 'connection failed ' . $! . ' ' . $DBI::errstr;

my $Timezone = $Opts{t} ? $Opts{t} : DateTime::TimeZone->new( name => "local")->name;

my $start = time();
#my $Outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');

my ($query,$sth);
$query = 'INSERT INTO imports (name, description, datatype) VALUES(?,?,?)';
$sth = $Dbh->prepare($query);
$sth->execute($Name, $Description, $Format);
my $Id = $Dbh->{mysql_insertid};

my $lines_imported = 0;
if ($Format eq 'local_syslog'){
	$lines_imported = _read_local_syslog();
}
elsif ($Format eq 'bro'){
	$lines_imported = _read_bro();
}
elsif ($Format eq 'snort'){
	$lines_imported = _read_snort();
}
# These are deprecated, use POST to /API/upload instead
#elsif ($Format eq 'preformatted_class_only'){
#	$lines_imported = _read_local_preformatted_class_only();
#}
#elsif ($Format eq 'preformatted'){
#	$lines_imported = _read_local_preformatted();
#}
#elsif ($Format eq 'csv'){
#	$lines_imported = _read_csv();
#}
#elsif ($Format eq 'tsv'){
#	$lines_imported = _read_tsv();
#}
elsif ($Format eq 'sidewinder'){
	$lines_imported = _read_sidewinder();
}
else {
	print "Using default format since none specified.\n";
	$lines_imported = _read_local();
}

my $end_time = time() - $start;
print "Sent $lines_imported lines to ELSA in $end_time seconds\n";

# Local flat-file syslog is fine as-is
sub _read_local_syslog {
	my $outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');
	my $counter = 0;
	while (<$Infile>){
		if ($. <= $Lines_to_skip){
			next;
		}
		$outfile->print($_);
		$counter++;
	}
	return $counter;
}

# Local file as-is and append a timestamp and program
sub _read_local {
	my $program = 'unknown';
	if ($Opts{p}){
		$program = $Opts{p};
	}
	my $program_id = crc32($program);
	
	# Record our program_id to the database if necessary
	$query = 'INSERT IGNORE INTO programs (id, program) VALUES(?,?)';
	$sth = $Dbh->prepare($query);
	$sth->execute($program_id, $program);
	
	my $date = strftime('%b %d %H:%M:%S', localtime(time()));
	
	my $outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');
	my $counter = 0;
	while (<$Infile>){
		if ($. <= $Lines_to_skip){
			next;
		}
		$outfile->print("$date $program: $_");
		$counter++;
	}
	return $counter;
}

# Sidewinder
sub _read_sidewinder {
	my $program = 'auditd';
	my $program_id = crc32($program);
	
	# Record our program_id to the database if necessary
	$query = 'INSERT IGNORE INTO programs (id, program) VALUES(?,?)';
	$sth = $Dbh->prepare($query);
	$sth->execute($program_id, $program);
	
	my $parser = DateTime::Format::Strptime->new(pattern => '%b %d %T %Y %Z', time_zone => $Timezone);
	my $printer = DateTime::Format::Strptime->new(pattern => '%b %d %T', time_zone => $Timezone);
	my $host = inet_ntoa(pack('N*', $Id));
	
	my $outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');
	my $counter = 0;
	while (<$Infile>){
		$_ =~ /^date="([^"]+)/;
		my $dt = $parser->parse_datetime($1) or next;
		$outfile->print($printer->format_datetime($dt) . " $host $program: $_");
		$counter++;
	}
	return $counter;
}

# Read from a Bro file
sub _read_bro {
	my $outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');
	my $counter = 0;
	$Infile_name =~ /([^\/\.]+)[\.\w]+$/;
	my $type = $1;
	while (<$Infile>){
		eval {
			chomp;
			next if $_ =~ /^#/;
			my @fields = split(/\t/, $_);
			my $second = $fields[0];
			($second) = split(/\./, $second, 1);
			my $date = strftime('%b %d %H:%M:%S', localtime($second));
			$outfile->print($date . " bro_$type: " . join('|', @fields) . "\n");
			$counter++;
		};
		if ($@){
			$Log->error($@ . "\nLine: " . $_);
		}
	}
	return $counter;
}

# Snort/Suricata fast.log
sub _read_snort {
	my $outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');
	my $counter = 0;
	my $parser = DateTime::Format::Strptime->new(pattern => '%m/%d/%Y-%T', time_zone => $Timezone);
	my $printer = DateTime::Format::Strptime->new(pattern => '%b %d %T', time_zone => $Timezone);
	while (<$Infile>){
		$_ =~ /^(\S+)\s+\[\*\*\] ([^\n]+)/;
		my $dt = $parser->parse_datetime($1);
		my $msg = $2;
		$msg =~ s/\[\*\*\]\ //;
		$outfile->print($printer->format_datetime($dt) . " snort: $msg\n");
		$counter++;
	}
	return $counter;
}

# Local flat-file already formatted, starting with class
sub _read_local_preformatted_class_only {
	# Create a new file which has the right host_id to represent the import id
	my $outfile_name = $Conf->{buffer_dir} . '/' . time() or die('Unable to create outfile: ' . $!);;
	my $outfile = new IO::File('> ' . $outfile_name);
	my $program = 'unknown';
	if ($Opts{p}){
		$program = $Opts{p};
	}
	my $program_id = crc32($program);
	
	# Record our program_id to the database if necessary
	$query = 'INSERT IGNORE INTO programs (id, program) VALUES(?,?)';
	$sth = $Dbh->prepare($query);
	$sth->execute($program_id, $program);
	
	my $counter = 0;
	my $default_time = time();
	while (<$Infile>){
		$_ =~ s/\\/\\\\/g;
		$outfile->print(join("\t", 0, $default_time, $Id, $program_id) . "\t$_");
		$counter++;
	}
	$outfile->close;
	my $indexer = new Indexer(log => $Log, conf => $Config_json);
	my $batch_ids  = $indexer->load_records({ file => $outfile_name, import => 1 });
	$batch_ids->{import} = 1;
	$indexer->index_records($batch_ids);
	
	return $counter;
}

# Local flat-file already formatted, replacing the host field with the import ID
sub _read_local_preformatted {
	# Create a new file which has the right host_id to represent the import id
	my $outfile_name = $Conf->{buffer_dir} . '/' . time();
	my $outfile = new IO::File('> ' . $outfile_name) or die('Unable to create outfile: ' . $!);
	if ($Opts{p}){
		# Record our program_id to the database if necessary
		$query = 'INSERT IGNORE INTO programs (id, program) VALUES(?,?)';
		$sth = $Dbh->prepare($query);
		my @programs_to_insert = split(/,/, $Opts{p});
		foreach my $program (@programs_to_insert){
			my $program_id = crc32($program);
			$sth->execute($program_id, $program);
		}
	}
	
	my $counter = 0;
	my $default_time = time();
	while (<$Infile>){
		chomp;
		$_ =~ s/\\/\\\\/g;
		my @line = (0, split(/\t/, $_));
		$line[2] = $Id;
		$outfile->print(join("\t", @line) . "\n");
		$counter++;
	}
	$outfile->close;
	my $indexer = new Indexer(log => $Log, conf => $Config_json);
	my $batch_ids  = $indexer->load_records({ file => $outfile_name, import => 1 });
	$batch_ids->{import} = 1;
	$indexer->index_records($batch_ids);
	
	return $counter;
}

# Read CSV
sub _read_csv {
	my $separator = shift;
	$separator ||= ',';
	die('Class is required (specific with -C)') unless $Opts{C};
	require Text::CSV;
	my $csv = new Text::CSV({
		sep_char => $separator,
		quote_char => "'",
		blank_is_undef => 1,
		empty_is_undef => 1,
	});
	# Create a new file which has the right host_id to represent the import id
	my $outfile_name = $Conf->{buffer_dir} . '/' . time();
	my $outfile = new IO::File('> ' . $outfile_name);
	my $program = 'unknown';
	if ($Opts{p}){
		$program = $Opts{p};
	}
	my $program_id = crc32($program);
	
	# Record our program_id to the database if necessary
	$query = 'INSERT IGNORE INTO programs (id, program) VALUES(?,?)';
	$sth = $Dbh->prepare($query);
	$sth->execute($program_id, $program);
	
	# Get columns from first line
	my $first_line = <$Infile>;
	chomp($first_line);
	my @cols = split(/,/, $first_line);
	
	$query = 'SELECT id FROM classes WHERE class=?';
	$sth = $Dbh->prepare($query);
	$sth->execute($Opts{C});
	my $row = $sth->fetchrow_hashref;
	die('Invalid class ' . $Opts{C} . ' given') unless $row;
	my $class_id = $row->{id};
	
	$query = 'SELECT field_order FROM fields_classes_map t1 JOIN fields t2 ON (t1.field_id=t2.id) ' .
		'WHERE t2.field=? AND t1.class_id=?';
	$sth = $Dbh->prepare($query);
	my @field_orders;
	for (my $i = 0; $i < @cols; $i++){
		if ($cols[$i] eq 'timestamp'){
			$field_orders[$i] = 1;
		}
		else {
			$sth->execute($cols[$i], $class_id);
			my $row = $sth->fetchrow_hashref;
			unless ($row){
				die('Unable to find a field_order for column ' . $cols[$i] . ' and class ' . $Opts{C});
			}
			$field_orders[$i] = $row->{field_order} + 1; # add one to account for the auto-inc value we need to add
		}
	}
	
	my $counter = 0;
	my $default_time = time();
	while (my $line = $csv->getline($Infile)){
		my @ordered_line = (0, $default_time, $Id, $program_id, $class_id, join(',', @$line),
			undef, undef, undef, undef, undef, undef, undef, undef, undef, undef, undef, undef);
		for (my $i = 0; $i < @$line; $i++){
			# Escape backslashes
			$line->[$i] =~ s/\\/\\\\/g;
			# Put this value in the correct field order for the schema
			$ordered_line[ $field_orders[$i] ] = $line->[$i];
		}
		
		$outfile->print(join("\t", @ordered_line) . "\n");
		$counter++;
	}
	$outfile->close;
	my $indexer = new Indexer(log => $Log, conf => $Config_json);
	my $batch_ids  = $indexer->load_records({ file => $outfile_name, import => 1 });
	$batch_ids->{import} = 1;
	$indexer->index_records($batch_ids);
	
	return $counter;
}

sub _read_tsv {
	return _read_csv("\t");
}

#sub _init_preformatted {
#	$Outfile->close;
#	# Create a new file which has the right host_id to represent the import id
#	my $outfile_name = $Conf->{buffer_dir} . '/' . time();
#	$Outfile = new IO::File('> ' . $outfile_name);
#	my $program = 'unknown';
#	if ($Opts{p}){
#		$program = $Opts{p};
#	}
#	my $program_id = crc32($program);
#	
#	# Record our program_id to the database if necessary
#	$query = 'INSERT IGNORE INTO programs (id, program) VALUES(?,?)';
#	$sth = $Dbh->prepare($query);
#	$sth->execute($program_id, $program);
#}

sub usage {
	print 'Usage: import.pl [ -c <conf file> ] [ -f <format> ] [ -s <lines to skip> ] <input file>' . "\n";
}