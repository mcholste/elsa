#!/usr/bin/perl

# This file is used to receive import files sent by log forwarders via HTTP.  It will
# receive the upload and process it as if the file were generated locally, including
# running all plugins.

use strict;
use Data::Dumper;
use Plack::Builder;
use Plack::Request;
use Plack::App::File;
use Plack::Builder::Conditionals;
use FindBin;
use lib $FindBin::Bin;
use Getopt::Std;
use Log::Log4perl;
use File::Copy;
use Archive::Extract;
use Digest::MD5;
use IO::File;
use Time::HiRes qw(time);

use Indexer;

my %Opts;
getopts('c:', \%Opts);

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
		InactiveDestroy => 1, 
		PrintError => 0,
		mysql_auto_reconnect => 1, 
		HandleError => \&_sql_error_handler,
		mysql_local_infile => 1,
	}) 
	or die 'connection failed ' . $! . ' ' . $DBI::errstr;

sub _sql_error_handler {
	my $errstr = shift;
	my $dbh = shift;
	my $query = $dbh->{Statement};
	my $full_errstr = 'SQL_ERROR: ' . $errstr . ', query: ' . $query; 
	$Log->error($full_errstr);
	#return 1; # Stops RaiseError
	die($full_errstr);
}

my $static_root = $FindBin::Bin . '/../';
if (exists $ENV{DOCUMENT_ROOT}){
	$static_root = $ENV{DOCUMENT_ROOT} . '/../';
}

builder {
	$ENV{PATH_INFO} = $ENV{REQUEST_URI}; #mod_rewrite will mangle PATH_INFO, so we'll set this manually here in case it's being used
	#enable 'ForwardedHeaders';
	enable 'NoMultipleSlashes';
	enable 'CrossOrigin', origins => '*', methods => '*', headers => '*';
	
	mount '/favicon.ico' => sub { return [ 200, [ 'Content-Type' => 'text/plain' ], [ '' ] ]; };
	mount '/' => sub {
		my $env = shift;
		my $req = Plack::Request->new($env);
		my $params = $req->parameters->as_hashref;
		my $uploaded_file = $req->uploads->{filename};
#		my $new_file_name = $Conf->{buffer_dir} . '/' . $req->address . '_' . $uploaded_file->basename;
#		move($uploaded_file->path, $new_file_name) or (
#			$Log->error('Unable to move ' . $uploaded_file->path . ' to ' . $new_file_name . ': ' . $!)
#			and return [ 500, [ 'Content-Type' => 'text/plain' ], [ 'error' ] ]
#		);
		
		$Log->debug('params: ' . Dumper($params));
		$Log->info('Received file ' . $uploaded_file->basename . ' with size ' . $uploaded_file->size 
			. ' from client ' . $req->address);
		my ($query, $sth);
		
		my $ae = Archive::Extract->new( archive => $uploaded_file->path );
		my $id = $req->address . '_' . $params->{md5};
		# make a working dir for these files
		my $working_dir = $Conf->{buffer_dir} . '/' . $id;
		mkdir($working_dir);
		$ae->extract( to => $working_dir ) or die($ae->error);
		my $files = $ae->files;
		foreach my $unzipped_file_shortname (@$files){
			my $unzipped_file = $working_dir . '/' . $unzipped_file_shortname;
			my $file = $Conf->{buffer_dir} . '/' . $id . '_' . $unzipped_file_shortname;
			move($unzipped_file, $file);
			
			if ($unzipped_file_shortname =~ /programs/){
				$Log->info('Loading programs file ' . $file);
				$query = 'LOAD DATA LOCAL INFILE ? INTO TABLE programs';
				$sth = $Dbh->prepare($query);
				$sth->execute($file);
				next;
			}
			
			# Check md5
			my $md5 = new Digest::MD5;
			my $upload_fh = new IO::File($file);
			$md5->addfile($upload_fh);
			my $local_md5 = $md5->hexdigest;
			close($upload_fh);
			unless ($local_md5 eq $params->{md5}){
				my $msg = 'MD5 mismatch! Found: ' . $local_md5 . ' expected: ' . $params->{md5};
				$Log->error($msg);
				unlink($file);
				return [ 400, [ 'Content-Type' => 'text/plain' ], [ $msg ] ];
			}
			unless ($params->{start} and $params->{end}){
				my $msg = 'Did not receive valid start/end times';
				$Log->error($msg);
				unlink($file);
				return [ 400, [ 'Content-Type' => 'text/plain' ], [ $msg ] ];
			}
			
			# Record our received file in the database
			$query = 'INSERT INTO buffers (filename, start, end) VALUES (?,?,?)';
			$sth = $Dbh->prepare($query);
			$sth->execute($file, $params->{start}, $params->{end});
			my $buffers_id = $Dbh->{mysql_insertid};
			
			# Record the upload
			$query = 'INSERT INTO uploads (client_ip, count, size, batch_time, errors, start, end, buffers_id) VALUES(INET_ATON(?),?,?,?,?,?,?,?)';
			$sth = $Dbh->prepare($query);
			$sth->execute($req->address, $params->{count}, $params->{size}, $params->{batch_time}, 
				$params->{total_errors}, $params->{start}, $params->{end}, $buffers_id);
			$sth->finish;
		}
		rmdir($working_dir);
		return [ 200, [ 'Content-Type' => 'text/plain' ], [ 'ok' ] ];
	};
};

