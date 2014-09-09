#!/usr/bin/perl
use strict;
use Data::Dumper;

# Include the directory this script is in
use FindBin;
use lib $FindBin::Bin;

# Log must go first to load its Moose dependencies before others
use Log;

use IO::Handle;
use POSIX;
use Config::JSON;
use Getopt::Std;
use String::CRC32;
use Log::Log4perl;
use DBI;
use JSON;
use IO::File;
use Digest::MD5;
use Time::HiRes;
use File::Copy;
use IO::Compress::Gzip qw(gzip $GzipError);

use Indexer;
use Reader;
use Writer;

use constant LIVETAIL_QID => 0;
use constant LIVETAIL_CLASSES => 1;
use constant LIVETAIL_PERMISSIONS => 2;
use constant LIVETAIL_AND => 3;
use constant LIVETAIL_OR => 4;
use constant LIVETAIL_NOT => 5;
use constant LIVETAIL_ANY_AND => 6;
use constant LIVETAIL_ANY_OR => 7;
use constant LIVETAIL_ANY_NOT => 8;

my $Field_to_sub = {
	classes => LIVETAIL_CLASSES,
	and => LIVETAIL_AND,
	or => LIVETAIL_OR,
	not => LIVETAIL_NOT,
	permissions => LIVETAIL_PERMISSIONS,
	any_field_terms_and => LIVETAIL_ANY_AND,
	any_field_terms_or => LIVETAIL_ANY_OR,
	any_field_terms_not => LIVETAIL_ANY_NOT,
};

my @String_fields = (Reader::FIELD_MSG, Reader::FIELD_S0, Reader::FIELD_S1, Reader::FIELD_S2, Reader::FIELD_S3, Reader::FIELD_S4, Reader::FIELD_S5);

my %Opts;
getopts('onlc:f:', \%Opts);

$| = 1;
my $pipes     = {};
my $Conf_file = $Opts{c} ? $Opts{c} : '/etc/elsa_node.conf';
my $Config_json = Config::JSON->new( $Conf_file );
my $Conf = $Config_json->{config}; # native hash is 10x faster than using Config::JSON->get()

# Setup logger
my $logdir = $Conf->{logdir};
my $debug_level = $Conf->{debug_level};
#my $l4pconf = qq(
#	log4perl.category.ELSA       = $debug_level, File
#	log4perl.appender.File			 = Log::Log4perl::Appender::File
#	log4perl.appender.File.filename  = $logdir/node.log
#	log4perl.appender.File.syswrite = 1
#	log4perl.appender.File.recreate = 1
#	log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
#	log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
#	log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
#	log4perl.filter.ScreenLevel.LevelMin  = $debug_level
#	log4perl.filter.ScreenLevel.LevelMax  = ERROR
#	log4perl.filter.ScreenLevel.AcceptOnMatch = true
#	log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
#	log4perl.appender.Screen.Filter = ScreenLevel 
#	log4perl.appender.Screen.stderr  = 1
#	log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
#	log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
#);
#Log::Log4perl::init( \$l4pconf ) or die("Unable to init logger\n");
#my $Log = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");

Log::config_logger($Config_json);
my $Log = Log::Log4perl::get_logger('App')
  or die("Unable to init logger");
# Don't log ops logs if this is the ops logger elsa.pl instance
if ($Opts{f} eq '__OPS__'){
	$Log->remove_appender('RFC5424') if Log::Log4perl->appenders->{RFC5424};
}

my $Dbh = _db_connect($Conf) or die 'connection failed ' . $! . ' ' . $DBI::errstr;

my $num_children = $Conf->{num_log_readers} or die("undefined config for num_log_readers");
my $continue     = 1;
my $Run          = 1;
my $Class_info = Reader::get_class_info($Dbh);
my $Cache = {};
_init_cache();
my %Forwarders = (
	'scp' => 'Forwarder::SSH',
	'cp' => 'Forwarder::Copy',
	'url' => 'Forwarder::URL',
);
if ($Config_json->get('forwarding/destinations')){
	foreach my $forwarding_hash (@{ $Config_json->get('forwarding/destinations') }){
		if ($forwarding_hash->{package}){
			$Forwarders{ $forwarding_hash->{method} } = $forwarding_hash->{package};
		}
	}
}
foreach my $forwarder_method (keys %Forwarders){
	eval {
		(my $file = $Forwarders{$forwarder_method}) =~ s|::|/|g; 
		require $file . '.pm';
		$Forwarders{$forwarder_method}->import();
		1;
	};
	if ($@){
		my $package = delete $Forwarders{$forwarder_method};
		$Log->error('Unable to use configured package ' . $package . ': ' . $@);
	}
	else {
		$Forwarders{$forwarder_method} = $Forwarders{$forwarder_method};
	}
}

unless (-f $Conf->{sphinx}->{config_file}){
	_create_sphinx_conf();
}

if ($Opts{f}){
	$Log->trace("Processing file $Opts{f}...");
	print "Processing file $Opts{f}...\n";
	if ($Opts{f} eq '__IMPORT__'){
		_import();
	}
	else {
		_process_batch($Opts{f});
	}
	exit;
}

unless ($Opts{n} or $Conf->{forwarding}->{forward_only}){
	print "Validating directory...\n";
	my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
	$indexer->initial_validate_directory();
}

if ($Opts{l} and not $Conf->{forwarding}->{forward_only}){
	print "Loading existing buffers\n";
	my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
	$indexer->load_buffers();
	exit;
}

if ($Opts{o}){
	print "Running once\n";
	$Run = 0;
}

$SIG{TERM} = sub { $Run = 0; $Log->warn('Shutting down'); };
$SIG{CHLD} = 'IGNORE'; # will do the wait() so we don't create zombies

my $total_processed = 0;
my $Realtime_enabled = defined $Conf->{realtime};
my $Realtime = $Realtime_enabled ? $Conf->{realtime} : undef;
my $rt_indexed = 0;
do {
	if ($Realtime_enabled){
		my ($num_processed, $current_id) = _realtime_process();
		$total_processed += $num_processed;
		$rt_indexed += $num_processed;
		
		# Check to see if we need to exit realtime mode
		if (($num_processed / $Conf->{sphinx}->{index_interval}) > $Realtime->{rate}){
			$Log->info('Leaving realtime processing because rate was ' . ($num_processed / $Conf->{sphinx}->{index_interval}));
			$Realtime_enabled = 0;
		}
	}
	else {
		$Log->debug("Starting process_batch");
		eval {
			my $num_processed = _process_batch();
			$total_processed += $num_processed;
			$Log->debug("Processed $num_processed records");
			# Check to see if we need to enter realtime mode
			if ($Realtime and $Realtime->{rate} and $Conf->{sphinx}->{index_interval} and 
				($num_processed / $Conf->{sphinx}->{index_interval}) < $Realtime->{rate}){
				$Log->info('Entering realtime processing because rate was ' . ($num_processed / $Conf->{sphinx}->{index_interval}));
				$Realtime_enabled = 1;
			}
			sleep 1 unless $num_processed; # avoid batch-bombing if our parent handle closes
		};
		if ($@) {
			my $e = $@;
			$Log->error($e);
			sleep 1 if $Run;                                # to avoid errmsg flooding
		}
	}
	$Conf = Config::JSON->new( $Conf_file )->{config} if $Run; # reload the config in case it has changed on disk
} while ($Run);

$Log->info('Exiting after processing ' . $total_processed . ' records');
exit;

sub _sql_error_handler {
	my $errstr = shift;
	my $dbh = shift;
	my $query = $dbh->{Statement};
	my $full_errstr = 'SQL_ERROR: ' . $errstr . ', query: ' . $query; 
	$Log->error($full_errstr);
	#return 1; # Stops RaiseError
	die($full_errstr);
}

sub _create_sphinx_conf {
	my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
	open(FH, '>' . $Conf->{sphinx}->{config_file}) or die("Cannot open config file for writing: $!");
	print FH $indexer->get_sphinx_conf();
	close(FH);
	print 'Wrote new config to file ' . $Conf->{sphinx}->{config_file} . "\n";
}

sub _process_batch {
	my $filename = shift;
	
	if (getppid == 1){
		$Log->error('We lost our parent process');
		exit; #die gets caught, we want a complete exit
	}
	
	my $args = {};
	my $fh = \*STDIN;
	my $offline_processing;
	my $is_ops;
	if ($filename){
		if ($filename eq '__OPS__'){
			$is_ops = 1;
		}
		else {
			unless ($filename eq '__IMPORT__'){
				open($fh, $filename) or die 'Unable to open file: ' . $!;
				$Log->debug('Reading from file ' . $filename);
			}
			$args->{offline_processing} = $offline_processing = 1;
			$args->{offline_processing_start} = time();
			$args->{offline_processing_end} = 0;
		}
	}
	$fh->autoflush(1);
	$fh->blocking(1);
	
	die "Non-existent buffer_dir: " . $Conf->{buffer_dir}
		unless -d $Conf->{buffer_dir};
		
	$args->{start_time} = Time::HiRes::time();
	$args->{error_counter} = 0;
	
	# Reset the miss cache
	$args->{cache_add} = {};
	my $tempfile_name = $Conf->{buffer_dir} . '/' . ($args->{offline_processing} ? 'import_' : '') . ($is_ops ? 'ops_' : '') . Time::HiRes::time();
	
#	my $tail_watcher = _fork_livetail_manager($tempfile_name) unless $Opts{o} or $args->{offline_processing};
	
	# Open the file now that we've forked
	my $tempfile;
	sysopen($tempfile, $tempfile_name, O_RDWR|O_CREAT) or die('Unable to open our tempfile: ' . $!);
	$Log->debug('Offline processing: ' . $args->{offline_processing} . ' and using tempfile ' . $tempfile_name); 
	
	my $run = 1;
	# End the loop after index_interval seconds
	local $SIG{ALRM} = sub {
		$Log->trace("ALARM");
		$run = 0;
		# safety in case we don't receive any logs, we'll still do post_proc and restart loop
		$fh->blocking(0); 
	};
	unless ($args->{offline_processing}){
		alarm $Conf->{sphinx}->{index_interval};
	}
	my $reader = new Reader(log => $Log, conf => $Config_json, cache => $Cache, offline_processing => $args->{offline_processing});

	my $batch_counter = 0; # we make this a standard variable instead of using $arg->{batch_counter} to save the hash deref in loop
	while (<$fh>){	
		eval { 
			$tempfile->syswrite(join("\t", 0, @{ $reader->parse_line($_) }) . "\n"); # tack on zero for auto-inc value
			$batch_counter++;
		};
		if ($@){
			my $e = $@;
			$args->{error_counter}++;
			if ($Conf->{log_parse_errors}){
				$Log->error($e) 
			}
		}
		last unless $run;
		# If importing, we want to stop as soon as we're done processing the file
		alarm(1) if $offline_processing;
	}
	close($tempfile);
			
	# Update args to be results
	$args->{batch_counter} = $batch_counter;
	$args->{file} = $tempfile_name;
	$args->{file_size} = -s $args->{file};
	$args->{start} = $reader->processing_times->{start};
	$args->{end} = $reader->processing_times->{end};
	$args->{total_processed} = $args->{batch_counter};
	$args->{total_errors} = $args->{error_counter};
	$args->{batch_time} = $Conf->{sphinx}->{index_interval};
	
	# Report back that we've finished
	$Log->debug("Finished job process_batch with $args->{batch_counter} logs processed and " . (scalar keys %{ $args->{cache_add} }) . ' new programs');
	$Log->debug('Total errors: ' . $args->{error_counter} . ' (%' . (($args->{error_counter} / $args->{batch_counter}) * 100) . ')' ) if $args->{batch_counter};
	$Log->debug('file size for file ' . $args->{file} . ' is ' . $args->{file_size});
	
#	# Kill the livetail forker
#	if ($tail_watcher){
#		kill SIGTERM, $tail_watcher;
#		$Log->trace("Ending child tail manager $tail_watcher");
#	}
	
	unless ($args->{batch_counter}){
		$Log->trace('No logs recorded');
		unlink ($args->{file}) if -f $args->{file};
		return $args->{batch_counter};
	}
	
	if ($is_ops){
		_forward($args, $reader, $is_ops);
		return $args->{batch_counter};
	}
	
	if (scalar keys %{ $reader->to_add }){
		my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
		$indexer->add_programs($reader->to_add);
		$reader->to_add({});
	}
	
	my ($query,$sth);
	if ($args->{batch_counter} and (not $Conf->{forwarding} or 
		($Conf->{forwarding} and not $Conf->{forwarding}->{forward_only}))){
		$query = 'INSERT INTO buffers (filename, start, end) VALUES (?,?,?)';
		$sth = $Dbh->prepare($query);
		$sth->execute($args->{file}, $args->{start}, $args->{end});
		$Log->trace('inserted filename ' . $args->{file} . ' with batch_counter ' . $args->{batch_counter} 
			. ' and start ' . (scalar localtime($args->{start})) . ' and end ' . (scalar localtime($args->{end})));
	}
	
	$Dbh->disconnect;
	
	_forward($args, $reader, $is_ops);
	
	return $args->{batch_counter};
}

sub _import {
	if (getppid == 1){
		$Log->error('We lost our parent process');
		exit; #die gets caught, we want a complete exit
	}
	
	my $args = {};
	my $fh = \*STDIN;
	$fh->autoflush(1);
	$fh->blocking(1);
	
	die "Non-existent buffer_dir: " . $Conf->{buffer_dir}
		unless -d $Conf->{buffer_dir};
		
	$args->{start_time} = Time::HiRes::time();
	$args->{error_counter} = 0;
	
	my %imports;
	
	my $reader = new Reader(log => $Log, conf => $Config_json, cache => $Cache, offline_processing => 1);

	my $run = 1;
	# End the loop after index_interval seconds
	local $SIG{ALRM} = sub {
		$Log->trace("Finished with import loop.");
		$run = 0;
		# safety in case we don't receive any logs, we'll still do post_proc and restart loop
		$fh->blocking(0); 
	};
	
	while (<$fh>){
		eval {		
			my $line = $reader->parse_line($_);
			if (not exists $imports{$Reader::IMPORT_ID}){
				$imports{$Reader::IMPORT_ID}->{args} = { %$args };
				$imports{$Reader::IMPORT_ID}->{args}->{file} = $Conf->{buffer_dir} . '/' . Time::HiRes::time();
				sysopen($imports{$Reader::IMPORT_ID}->{fh}, $imports{$Reader::IMPORT_ID}->{args}->{file}, O_RDWR|O_CREAT) or die('Unable to open our tempfile: ' . $!);
			}
			$imports{$Reader::IMPORT_ID}->{fh}->syswrite(join("\t", 0, @$line) . "\n"); # tack on zero for auto-inc value
			$imports{$Reader::IMPORT_ID}->{args}->{batch_counter}++;
		};
		if ($@){
			my $e = $@;
			$imports{$Reader::IMPORT_ID}->{args}->{error_counter}++;
			if ($Conf->{log_parse_errors}){
				$Log->error($e) 
			}
		}
		last unless $run;
		# We want to stop as soon as we're done processing the file
		alarm(1);
	}
	
	foreach my $import_id (keys %imports){
		my $import_args = $imports{$import_id}->{args};
		# Update args to be results
		$import_args->{file_size} = -s $import_args->{file};
		$import_args->{start} = $reader->processing_times->{start};
		$import_args->{end} = $reader->processing_times->{end};
		$import_args->{total_processed} = $args->{batch_counter};
		$import_args->{total_errors} = $import_args->{error_counter};
		$import_args->{batch_time} = $Conf->{sphinx}->{index_interval};
		
		# Report back that we've finished
		$Log->debug("Finished import ID $import_id process_batch with $import_args->{batch_counter} logs processed and " . (scalar keys %{ $reader->cache }) . ' new programs');
		$Log->debug('Total errors: ' . $import_args->{error_counter} . ' (%' . (($import_args->{error_counter} / $import_args->{batch_counter}) * 100) . ')' ) if $import_args->{batch_counter};
		$Log->debug('file size for file ' . $import_args->{file} . ' is ' . $import_args->{file_size});
		
		unless ($import_args->{batch_counter}){
			$Log->trace('No logs recorded');
			unlink ($import_args->{file}) if -f $import_args->{file};
			next;
		}
		
		if (scalar keys %{ $reader->to_add }){
			my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
			$indexer->add_programs($reader->to_add);
			$reader->to_add({});
		}
		
		my ($query,$sth);
		if ($import_args->{batch_counter} and (not $Conf->{forwarding} or 
			($Conf->{forwarding} and not $Conf->{forwarding}->{forward_only}))){
			$query = 'INSERT INTO buffers (filename, start, end, import_id) VALUES (?,?,?,?)';
			$sth = $Dbh->prepare($query);
			$sth->execute($import_args->{file}, $import_args->{start}, $import_args->{end}, $import_id);
			$Log->trace('inserted filename ' . $import_args->{file} . ' with import_id ' . $import_id . ' and batch_counter ' . $import_args->{batch_counter} 
				. ' and start ' . (scalar localtime($import_args->{start})) . ' and end ' . (scalar localtime($import_args->{end})));
		}
	}
	
	$Dbh->disconnect;
	
	foreach my $import_id (keys %imports){
		_forward($imports{$import_id}->{args}, $reader);
	}
}

sub _forward {
	my $args = shift;
	my $reader = shift;
	my $is_ops = shift;
	my ($query, $sth);
	
	# Are we forwarding events?
	if ($Conf->{forwarding}){
#		require Archive::Zip;
		my $pid = fork();
		if ($pid){
			# Parent
			if ($Conf->{forwarding}->{forward_only}){
				return $args->{batch_counter};
			}
		}
		else {
			# Child
			$Log->trace('Child started');
			# Get a new dbh handle independent of the parent's
			$Dbh = _db_connect($Conf) or die 'connection failed ' . $! . ' ' . $DBI::errstr;
			eval {
				my $md5_start = time();
				my @files_to_forward;
				
				$args->{compressed} = 1;
				
				# Write the new programs to a file
				if (scalar keys %{ $reader->to_add }){
					my ($program_fh, $program_filename);
					$program_filename = $Conf->{buffer_dir} . '/programs_' . time();
					sysopen($program_fh, $program_filename, O_RDWR|O_CREAT);
					foreach my $program (keys %{ $reader->to_add }){
						$program_fh->print(join("\t", $reader->to_add->{$program}->{id}, $program) . "\n");
					}
					close($program_fh);
					
					gzip $program_filename => "$program_filename.gz" or die($GzipError);
					push @files_to_forward, { file => "$program_filename.gz", program_file => "$program_filename.gz", original_file => $program_filename };
				}
				
				my $compressed_filename = $args->{file} . '.gz';
				my $start = Time::HiRes::time();
				gzip $args->{file} => $compressed_filename or die($GzipError);
				$Log->debug('file size for $compressed_filename ' . $compressed_filename . ': ' . (-s $compressed_filename));
				$args->{original_file} = $args->{file};
				$args->{file} = $compressed_filename;
				push @files_to_forward, $args;
				
				$Log->debug('files_to_forward: ' . Dumper(\@files_to_forward));
				
				foreach my $file_args (@files_to_forward){
					$Log->debug('file size for compressed file ' . $file_args->{file} . ': ' . (-s $file_args->{file}));
					# Calculate the MD5
					my $md5 = new Digest::MD5;
					my $check_fh = new IO::File($file_args->{file}) or die($file_args->{file} . ': ' . $!);
					$md5->addfile($check_fh);
					$file_args->{md5} = $md5->hexdigest;
					close($check_fh);
					my $md5_time_taken = time() - $md5_start;
					$Log->trace('Calculated gz file md5 ' . $file_args->{md5} . ' in ' . $md5_time_taken . ' seconds.');
					
					my $original_file = delete $file_args->{original_file};
					
					# Move the buffer file and new program file to remote location
					my $forwarding_errors = 0;
					foreach my $dest_hash (@{ $Conf->{forwarding}->{destinations} }){	
						my $forwarder;
						$Log->debug('is ops: ' . $is_ops);
						next if ($is_ops and not exists $dest_hash->{ops}) or (not $is_ops and exists $dest_hash->{ops});
						my $package = $Forwarders{ $dest_hash->{method} };
						if ($package){
							$forwarder = $package->new(log => $Log, conf => $Config_json, %{ $dest_hash });
						}
						else {
							$Log->error('Invalid or no forward method given, unable to forward logs, args: ' . Dumper($dest_hash));
							next;
						}
						my $ok = $forwarder->forward($file_args);
						$Log->debug('ok: ' . $ok);
						unless ($ok){
							$forwarding_errors++;
							$query = 'INSERT IGNORE INTO failed_buffers (hash, dest, args) VALUES (MD5(?),?,?)';
							$sth = $Dbh->prepare($query);
							$sth->execute(encode_json($file_args), encode_json($dest_hash), encode_json($file_args));
						}
					}
					
					if ($forwarding_errors){
						#move($compressed_filename, $compressed_filename . '_FORWARDING_FAILED');
					}
					else {
						# Delete our forward zip file
						unlink($file_args->{file});	
					}
					
					if ($file_args->{program_file} or $Conf->{forwarding}->{forward_only} or $is_ops){
						unlink($original_file);
					}
				}
			
				# Retry any fails from this or any other session
				$query = 'UPDATE failed_buffers SET pid=? WHERE ISNULL(pid)';
				$sth = $Dbh->prepare($query);
				$sth->execute($$);
				$query = 'SELECT hash, dest, args FROM failed_buffers WHERE pid=?';
				$sth = $Dbh->prepare($query);
				$sth->execute($$);
				my @rows;
				while (my $row = $sth->fetchrow_hashref){
					my $forwarder;
					my $dest_hash = decode_json($row->{dest});
					my $file_args = decode_json($row->{args});
					next if ($is_ops and not exists $dest_hash->{ops}) or (not $is_ops and exists $dest_hash->{ops});
					# Verify this destination is still valid
					my $found = 0;
					foreach my $config_dest_hash (@{ $Conf->{forwarding}->{destinations} }){
						my $forwarder_package = $Forwarders{ $dest_hash->{method} };
						my $identifiers = $forwarder_package->identifiers;
						if ($dest_hash->{method} eq $config_dest_hash->{method}){
							my $matched = 0;
							foreach my $identifier (@$identifiers){
								if ($dest_hash->{$identifier} eq $config_dest_hash->{$identifier}){
									$matched++;
								}
							}
							if ($matched == scalar @$identifiers){
								$found = 1;
							}
						}
					}
					if (not $found){
						$Log->warn('Retry destination of ' . $row->{dest} . ' no longer needed, removing retry.');
						$query = 'DELETE FROM failed_buffers WHERE hash=?';
						my $sth2 = $Dbh->prepare($query);
						$sth2->execute($row->{hash});
						$sth2->finish;
						next;
					}
					$Log->info('Retrying forward of file ' . $file_args->{file} . ' with args ' . Dumper($dest_hash));
					my $package = $Forwarders{ $dest_hash->{method} };
					if ($package){
						$forwarder = $package->new(log => $Log, conf => $Config_json, %{ $dest_hash });
					}
					else {
						$Log->error('Invalid or no forward method given, unable to forward logs, args: ' . Dumper($dest_hash));
						next;
					}

					my $ok = $forwarder->forward($file_args);
					if ($ok){
						$query = 'DELETE FROM failed_buffers WHERE hash=?';
						my $sth2 = $Dbh->prepare($query);
						$sth2->execute($row->{hash});
						$sth2->finish;
						# Delete our forward zip file
						unlink($file_args->{file});
					}
					else {
						$Log->error('Failed once again to forward file ' . $file_args->{file});
					}
					sleep 1; # sleep here to avoid flooding our upstream receiver
				}
				
				# Find any that have failed for too long
				$query = 'SELECT hash, args FROM failed_buffers WHERE timestamp < DATE_SUB(NOW(), INTERVAL ? SECOND)';
				$sth = $Dbh->prepare($query);
				my $timeout_seconds = 86400;
				if ($Conf->{forwarding}->{retry_timeout}){
					$timeout_seconds = $Conf->{forwarding}->{retry_timeout};
				}
				$sth->execute($timeout_seconds);
				while (my $row = $sth->fetchrow_hashref){
					my $file_args = decode_json($row->{args});
					$Log->warn('Retry timeout hit of ' . $timeout_seconds . ' seconds, abandoning buffer ' . $file_args->{file});
					$query = 'DELETE FROM failed_buffers WHERE hash=?';
					my $sth2 = $Dbh->prepare($query);
					$sth2->execute($row->{hash});
					$sth2->finish;
					move($file_args->{file}, $file_args->{file} . '_FORWARDING_FAILED');
				}
				# Remove our lock
				$query = 'UPDATE failed_buffers SET pid=NULL WHERE pid=?';
				$sth = $Dbh->prepare($query);
				$sth->execute($$);
			};
			if ($@){
				$Log->error('Child encountered error: ' . $@);
				# Remove our lock
				$query = 'UPDATE failed_buffers SET pid=NULL WHERE pid=?';
				$sth = $Dbh->prepare($query);
				$sth->execute($$);
			}
			
			$Log->trace('Child finished');
			exit; # done with child
		}
	}
	elsif ($is_ops){
		$Log->trace('Not forwarding ops file ' . $args->{file} . ', deleting');
		unlink($args->{file});
	}
}

sub _realtime_process {
	my $args = { run => 1, start_time => Time::HiRes::time() };
	
	my $fh = \*STDIN;
	$fh->autoflush(1);
	$fh->blocking(1);
	
	# End the loop after index_interval seconds
	local $SIG{ALRM} = sub {
		$Log->trace("ALARM");
		$args->{run} = 0;
		# check to see if we need to stop processing
		$fh->blocking(0);
	};
	alarm $Conf->{sphinx}->{index_interval};
			
	my $reader = new Reader(log => $Log, conf => $Config_json, cache => $Cache);
	my $writer = new Writer(log => $Log, conf => $Config_json);
	
	while (<$fh>){	
		eval { 
			$writer->write($reader->parse_line($_));
			$args->{batch_counter}++;
		};
		if ($@){
			my $e = $@;
			$args->{error_counter}++;
			if ($Conf->{log_parse_errors}){
				$Log->error($e) 
			}
		}
		last unless $args->{run};
	}
	
	# Disable the alarm if we haven't already
	alarm 0;
	
	# Insert any that are pending
	$writer->realtime_batch_insert();
	
	return ($args->{batch_counter}, $writer->current_id);
}

sub _init_cache {
	my ($query, $sth);
	$query = 'SELECT id, program FROM programs';
	$sth = $Dbh->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		$Cache->{ $row->{program} } = $row->{id};
	}
}

sub _fork_livetail_manager {
	my $filename = shift;
	my $pid = fork();
	if ($pid){
		$Log->trace('Forked livetail manager to pid ' . $pid . ' with filename ' . $filename);
		return $pid;
	}
	else {
		# Safety to exit after twice the normal batch time
		local $SIG{ALRM} = sub {
			$Log->trace("Emergency tail child exit");
			exit; 
		};
		alarm($Conf->{sphinx}->{index_interval} * 2);
		
		my $start_time = time();
		my %livetails;
		
		local $SIG{TERM} = sub {
			$Log->trace("Livetail manager received TERM signal");
			foreach my $qid (keys %livetails){
				$Log->trace('Killing ' . $livetails{$qid});
				kill SIGTERM, $livetails{$qid};
			}
			exit;
		};
		
		# Safety in case something goes horribly wrong to avoid forkbomb
		eval {
			# Get active livetail queries
			my ($query,$sth);
			do {
				$query = 'SELECT qid, query FROM livetail';
				$sth = $Dbh->prepare($query);
				$sth->execute;
				my %active_tails;
				while (my $row = $sth->fetchrow_hashref){
					$active_tails{ $row->{qid} } = $row;
				}
				$sth->finish; # finish this so the child doesn't pick it up
				foreach my $qid (keys %livetails){
					unless (exists $active_tails{$qid}){
						$Log->info('Removing inactive tail ' . $qid . ' at pid ' . $livetails{$qid});
						my $killed = kill SIGTERM, $livetails{$qid};
						if ($killed){
							$Log->trace('killed ' . $killed . ' procs');
							delete $livetails{$qid};
						}
						else {
							$Log->error('Unable to kill pid ' . $livetails{$qid});
						}
					}
				}
				foreach my $qid (keys %active_tails){
					next if $livetails{ $qid };
					my $livetail = _get_livetail($active_tails{$qid});
					$livetails{$qid} = _livetail($livetail, $filename);
					$Log->info('Added livetail ' . $qid . ' at pid ' . $livetails{$qid} . ' with filename ' . $filename);
				}
				sleep 1;
			} while ((time() - $Conf->{sphinx}->{index_interval}) < $start_time);
			
			$Log->trace("Livetail manager finished loop");
			foreach my $qid (keys %livetails){
				$Log->trace('Killing ' . $livetails{$qid});
				kill SIGTERM, $livetails{$qid};
			}
			exit;
		};
		if ($@){
			$Log->error("Error in fork manager: $@");
			sleep 1;
		}
	}
}

sub _livetail {
	my $livetail = shift;
	my $filename = shift;
	
	unless (-f $filename){
		$Log->error('Invalid filename ' . $filename);
		return;
	}
	
	my $pid = fork();
	if ($pid){
		$Log->trace('Forked livetail to pid ' . $pid);
		return $pid;
	}
	else {
		# Override parent's TERM
		local $SIG{TERM} = sub {
			$Log->trace("Livetail received TERM signal");
			exit;
		};
		
		my $writer = new Writer(log => $Log, conf => $Config_json);
		my $io = new IO::File($filename);
		$io->seek(0, SEEK_END);
		while (1){
			last unless stat($filename);
			while( my $line = $io->getline() ){
				_livetail_match_line($writer, $livetail, split(/\t/, $line));
			}
			sleep 1;
		}
		exit;
	}
}

sub _livetail_match_line {
	my $writer = shift;
	my $livetail_ref = shift;
	my @livetail = @$livetail_ref;
	my @line = @_;
	shift(@line); # burn off the 0 id
	
	foreach my $test (@{ $livetail[LIVETAIL_CLASSES] }){
		return unless $test->($line[Reader::FIELD_CLASS_ID]);
	}
		
	my $or_hits = 0;
	my $or_tests = 0;
	for (my $i = 0; $i <= Reader::FIELD_S5; $i++){
		next if $i == Reader::FIELD_CLASS_ID; # already handled above
		foreach my $test (@{ $livetail[LIVETAIL_PERMISSIONS]->[$i] }){
			return unless $test->($line[ Reader::FIELD_CLASS_ID ], $line[$i]);
		}
		foreach my $test (@{ $livetail[LIVETAIL_AND]->[$i] }){
			return unless $test->($line[ Reader::FIELD_CLASS_ID ], $line[$i]);
		}
		foreach my $test (@{ $livetail[LIVETAIL_NOT]->[$i] }){
			return if $test->($line[ Reader::FIELD_CLASS_ID ], $line[$i]);
		}
		foreach my $test (@{ $livetail[LIVETAIL_OR]->[$i] }){
			$or_tests++;
			$or_hits++ and last if $test->($line[ Reader::FIELD_CLASS_ID ], $line[$i]);
		}
	}
	if ($or_tests){
		return unless ($or_hits);
	}
	
	foreach my $test (@{ $livetail[LIVETAIL_ANY_AND] }){
		my $and_hits = 0;
		foreach my $i (@String_fields){
			next unless defined $line[$i];
			if ($test->($line[ Reader::FIELD_CLASS_ID ], $line[$i])){
				$and_hits++;
				next;
			}
		}
		return unless ($and_hits);
	}
	
	$or_tests = 0;
	$or_hits = 0;
	foreach my $i (@String_fields){
		next unless defined $line[$i];
		foreach my $test (@{ $livetail[LIVETAIL_ANY_NOT]}){
			return if $test->($line[ Reader::FIELD_CLASS_ID ], $line[$i]);
		}
		
		foreach my $test (@{ $livetail[LIVETAIL_ANY_OR] }){
			$or_tests++;
			$or_hits++ and last if $test->($line[ Reader::FIELD_CLASS_ID ], $line[$i]);
		}
	}
	if ($or_tests){
		return unless ($or_hits);
	}
	
	$line[0] = time();
	# Explicitly place undefs for otherwise non-existent fields to make sure there are a static number of values for placeholders
	for (my $i = Reader::FIELD_S0; $i <= Reader::FIELD_S5; $i++){
		unless ($line[$i] ne ''){
			$line[$i] = undef;
		}
	}
	unless (scalar @line == (Reader::FIELD_S5 + 1)){
		$Log->error('Invalid line passed match: ' . join(',', @line));
		return;
	}
	
	$writer->livetail_insert($livetail[LIVETAIL_QID], 0, @line);
}

sub _get_livetail {
	my $livetail = shift;
	my ($query, $sth);
#	$livetail->{query} = thaw($livetail->{query});
	$livetail->{query} = decode_json($livetail->{query});
	$Log->info('Livetail qid: ' . $livetail->{qid} . ', query: ' . Dumper($livetail->{query}));
	my @livetail_arr = ($livetail->{qid});
	
	foreach my $clause (qw(any_field_terms_and any_field_terms_or any_field_terms_not classes)){
		my @arr;
		foreach my $expression (@{ $livetail->{query}->{$clause} }){
			push @arr, eval($expression);
			if ($@){
				$Log->error('Failed to eval expression: ' . Dumper($expression));
				shift(@arr);
			}
		}
		$livetail_arr[ $Field_to_sub->{$clause} ] = [ @arr ];
	}
	foreach my $clause (qw(and or not permissions)){
		for (my $i = 0; $i < scalar @{ $livetail->{query}->{$clause} }; $i++){
			my @arr;
			foreach my $expression (@{ $livetail->{query}->{$clause}->[$i] }){
				push @arr, eval($expression);
				if ($@){
					$Log->error('Failed to eval expression: ' . $@ . ' ' . Dumper($expression));
					shift(@arr);
				}
			}
			$livetail_arr[ $Field_to_sub->{$clause} ] ||= [];
			$livetail_arr[ $Field_to_sub->{$clause} ]->[$i] = [ @arr ];
		}
	}

	return \@livetail_arr;
}

sub _db_connect {
	my $conf = shift;
	return DBI->connect(($conf->{database}->{dsn} or 'dbi:mysql:database=syslog;'), 
		$conf->{database}->{username}, 
		$conf->{database}->{password}, 
		{
			InactiveDestroy => 1, 
			PrintError => 0,
			mysql_auto_reconnect => 1, 
			HandleError => \&_sql_error_handler,
			mysql_local_infile => 1,
		});
};
