#!/usr/bin/perl
use strict;
use Data::Dumper;
use Getopt::Std;
use DateTime;
use DateTime::Format::Strptime;
use Reader;
use File::Temp;
use Config::JSON;
use String::CRC32;
use Time::HiRes qw(time);

my %Opts;
my $Infile = pop(@ARGV);
die('No infile given ' . usage()) unless -f $Infile;
getopts('c:l:t:', \%Opts);
print "Working on $Infile\n";
die('Cannot locate log2timeline, set with -l ' . $Opts{l} . usage()) unless -f $Opts{l};

$Opts{l} =~ /(.+)\/.+$/;
my $l2t_path = $1 . '/lib';

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

my $Timezone = $Opts{t} ? $Opts{t} : DateTime::TimeZone->new( name => "local")->name;
my $parser = DateTime::Format::Strptime->new(pattern => '%m/%d/%Y%t%T', time_zone => $Timezone);
my $dt = $parser->parse_datetime('08/26/2012 16:13:58');
my $printer = DateTime::Format::Strptime->new(pattern => '%s', time_zone => $Timezone);

my @Cols_to_collect = (qw(macb sourcetype user host desc notes));

my $reader = new Reader(log => $Log, conf => $Config_json, cache => { 'log2timeline' => crc32('log2timeline') }, offline_processing => 1);
$Reader::Log_parse_errors = 1;
my $tempfile = File::Temp->new( DIR => $Conf->{buffer_dir}, UNLINK => 0 );
my $batch_counter = 0;
my $error_counter = 0;
my $start = time();

my @col_names;
open(LOG2TIMELINE, '-|', 'perl -I' . $l2t_path . ' ' . $Opts{l} . ' -z ' . $Timezone . ' ' . $Infile);
while (<LOG2TIMELINE>){
	chomp;
	if ($. == 1){
		@col_names = split(/,/, $_);
		next;
	}
	
	my @line = split(/,/, $_);
	my $dt = $parser->parse_datetime($line[0] . ' ' . $line[1]);
	
	my %hash;
	@hash{@col_names} = @line;
	my %row;
	$row{timestamp} = $printer->format_datetime($dt);
	$row{program} = 'log2timeline';
	$row{class_id} = $reader->class_info->{classes}->{LOG2TIMELINE};
	$row{msg} = $_; #join(' ', @hash{@Cols_to_collect});
	
	foreach my $col (@Cols_to_collect){
		$row{$col} = $hash{$col};
	}
	$row{host} = ($hash{host} ne '-' and $hash{host}) ? $hash{host} : '127.0.0.1';
	$row{hostname} = delete $hash{host};
	
	eval { 
		$tempfile->print(join("\t", 0, @{ $reader->parse_hash(\%row) }) . "\n"); # tack on zero for auto-inc value
		#print(join("\t", 0, @{ $reader->parse_hash(\%row) }) . "\n");
		$batch_counter++;
	};
	if ($@){
		my $e = $@;
		$error_counter++;
		if ($Conf->{log_parse_errors}){
			$Log->error($e) 
		}
	}
}
print "Finished reading " . $tempfile->filename . " in " . $batch_counter/(time() - $start) . " records per second\n";
my $file = $tempfile->filename;
my @output = qx(perl elsa.pl -c $Conf_file -f $file);
unlink($tempfile->filename);
print "Finished loading into ELSA\n";

sub usage {
	print 'Usage: log2timeline2elsa.pl [ -c <conf file> ] -l <path to log2timeline> <file to give to log2timeline>' . "\n";
}