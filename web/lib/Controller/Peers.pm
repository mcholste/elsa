package Controller::Peers;
use Moose;
extends 'Controller';
use Data::Dumper;
use Log::Log4perl::Level;
use AnyEvent::HTTP;
use URI::Escape qw(uri_escape);
use File::Copy;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use Digest::MD5;
use IO::File;
use Time::HiRes qw(time);
use Hash::Merge::Simple qw(merge);
use File::Path;
use Try::Tiny;
use Ouch qw(:trytiny);
use Archive::Extract;

use lib qw(../);
use Utils;
use QueryParser;

use Import;

#our $Query_time_batch_threshold = 120;

sub local_info {
	my ($self, $args, $cb) = @_;
	
	try {
		my $ret;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub { $cb->($ret) });
		$self->_get_info(1, sub {
			$ret = shift;
			$cv->end;
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub local_stats {
	my ($self, $args, $cb) = @_;
	
	try {
		my $ret;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub { $cb->($ret) });
		$self->get_stats($args, sub {
			$ret = shift;
			$cv->end;
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub stats {
	my ($self, $args, $cb) = @_;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer unless $peer eq $args->{from_peer};
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my %results;
	my %stats;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { 
		$stats{overall} = (time() - $overall_start);
		$self->log->debug('stats: ' . Dumper(\%stats));
		$self->log->debug('merging: ' . Dumper(\%results));
		my $overall_final = merge values %results;
		$cb->(\%results);
	});
	
	foreach my $peer (@peers){
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_stats' : 'stats';
		$url .= '?start=' . uri_escape($args->{start}) . '&end=' . uri_escape($args->{end});
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		# Do not proxy localhost requests
		my @no_proxy = ();
		if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
			push @no_proxy, proxy => undef;
		}
		$results{$peer} = http_get $url, headers => $headers, @no_proxy, sub {
			my ($body, $hdr) = @_;
			try {
				my $raw_results = $self->json->decode($body);
				$stats{$peer}->{total_request_time} = (time() - $start);
				$results{$peer} = { %$raw_results }; #undef's the guard
				# Touch up nodes to have the correct label
				foreach my $node (keys %{ $results{$peer}->{nodes} }){
					if (($peer eq 'localhost' or $peer eq '127.0.0.1') and $args->{peer_label}){
						$results{$peer}->{nodes}->{ $args->{peer_label} } = delete $results{$peer}->{nodes}->{$node};
					}
					elsif ($node eq 'localhost' or $node eq '127.0.0.1'){
						$results{$peer}->{nodes}->{$peer} = delete $results{$peer}->{nodes}->{$node};
					}
				}
			}
			catch {
				my $e = catch_any(shift);
				$self->log->error($e->message . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $e->message, { http => $peer });
				delete $results{$peer};
			};
			$cv->end;
		};
	}
	$cv->end;
}

sub upload {
	my ($self, $args, $cb) = @_;
	
	$self->log->info('Received file ' . $args->{upload}->basename . ' with size ' . $args->{upload}->size 
		. ' from client ' . $args->{client_ip_address});
	my ($query, $sth);
	
	my $syslog_db_name = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$syslog_db_name = $self->conf->get('syslog_db_name');
	}
	
	my $ret = { ok => 1 };
	
	try {
	
		# See if this is a Zip file
		open(FH, $args->{upload}->path) or throw(500, 'Unable to read file ' . $args->{upload}->path . ': ' . $!, { file => $args->{upload}->path });
		my $buf;
		read(FH, $buf, 2);
		my $is_zipped = 0;
		# Check for zip or gz magic
		if ($buf eq 'PK'){
			$is_zipped = 2;
			#unlink($args->{upload}->path)
			#throw(400, 'Use separate .gz files, not a single .zip file for upload', { format => 'zip' });
		}
		elsif($buf eq pack('C2', 0x1f, 0x8b)){
			$self->log->trace('Detected that file upload is an archive');
			$is_zipped = 1;
		}
		close(FH);
		
		# Check md5
		my $md5 = new Digest::MD5;
		my $upload_fh = new IO::File($args->{upload}->path);
		$md5->addfile($upload_fh);
		my $local_md5 = $md5->hexdigest;
		close($upload_fh);
		if ($is_zipped != 2 and $local_md5 ne $args->{md5}){
			my $msg = 'MD5 mismatch! Calculated: ' . $local_md5 . ' client said it should be: ' . $args->{md5};
			$self->log->error($msg);
			unlink($args->{upload}->path);
			throw(400, $msg);
		}
		
		my $file;
		
		if ($is_zipped == 2){
			my $id = $args->{client_ip_address} . '_' . time;
			my $ae = Archive::Extract->new( archive => $args->{upload}->path ) or throw(500, 'Error extracting file ' . $args->{upload}->path . ': ' . $!, { file => $args->{upload}->path });
			# make a working dir for these files
			my $working_dir = $self->conf->get('buffer_dir') . '/' . $id;
			if (not mkdir($working_dir)){
				my $errmsg = $!;
				if ($errmsg eq 'File exists'){
					$self->log->warn('Dir ' . $working_dir . ' already existed');
					rmtree($working_dir);
					mkdir($working_dir) or throw(500, "Unable to create working_dir $working_dir: $!", { working_dir => $working_dir });
				}
				else {
					throw(500, "Unable to create working_dir $working_dir: $errmsg", { working_dir => $working_dir });
				}
			}
			$ae->extract( to => $working_dir ) or throw(500, $ae->error, { working_dir => $working_dir });
			my $files = $ae->files;
			if (scalar @$files > 2){
				$self->log->warn('Received more than 2 files in zip file, there should be at most one file and an optional programs file in a single zip file.');
			}
			foreach my $unzipped_file_shortname (@$files){
				my $unzipped_file = $working_dir . '/' . $unzipped_file_shortname;
				my $new_file = $self->conf->get('buffer_dir') . '/' . $id . '_' . $unzipped_file_shortname;
				move($unzipped_file, $new_file);
	
				if ($unzipped_file_shortname =~ /programs/){
					$self->log->info('Loading programs file ' . $new_file);
					$query = 'LOAD DATA LOCAL INFILE ? INTO TABLE ' . $syslog_db_name . '.programs';
					$sth = $self->db->prepare($query);
					$sth->execute($new_file);
					unlink($new_file);
					next;
				}
				$file = $new_file;
				
				# Check md5
				my $md5 = new Digest::MD5;
				my $upload_fh = new IO::File($file);
				$md5->addfile($upload_fh);
				my $local_md5 = $md5->hexdigest;
				if ($local_md5 ne $args->{md5}){
					my $msg = 'MD5 mismatch! Calculated: ' . $local_md5 . ' client said it should be: ' . $args->{md5};
					$self->log->error($msg);
					unlink($args->{upload}->path);
					throw(400, $msg);
				}
				$upload_fh->close;
			}
			rmtree($working_dir);
			$self->log->trace('Deleting original zip file ' . $args->{upload}->path);
			unlink($args->{upload}->path);
		}
		elsif ($is_zipped){
			my $id = $args->{client_ip_address} . '_' . $args->{md5};
			$file = $self->conf->get('buffer_dir') . '/' . $id;
			gunzip $args->{upload}->path => $file
				or throw(500, 'Error extracting file ' . $args->{upload}->path . ': ' . $!, { file => $args->{upload}->path });
			unlink($args->{upload}->path);
		}
		else {
			$file = $args->{upload}->path;
			$file =~ /\/([^\/]+)$/;
			my $shortname = $1;
			my $destfile = $self->conf->get('buffer_dir') . '/' . $shortname;
			move($file, $destfile) or throw(500, $!, { file => $file, destfile => $destfile });
			$self->log->debug('moved file ' . $file . ' to ' . $destfile);
			$file = $destfile;
		}
		
		# Sanity check the first 1k of the file to make sure we have no nulls (indicative of a mistakenly uploaded tarball)
		open(FH, $file) or throw(500, $!, { file => $file});
		read(FH, $buf, 1024);
		close(FH);
		if ($buf =~ /\0/){
			throw(400, 'File contains nulls, invalid.', { file => $file });
		}
		
		if ($args->{program_file}){
			$self->log->info('Loading programs file ' . $file);
			$query = 'LOAD DATA LOCAL INFILE ? INTO TABLE ' . $syslog_db_name . '.programs FIELDS ESCAPED BY \'\'';
			$sth = $self->db->prepare($query);
			$sth->execute($file);
			unlink($file);
		}
		elsif ($args->{stats_file}){
			$self->log->info('Loading host_stats file ' . $file);
			$query = 'LOAD DATA LOCAL INFILE ? INTO TABLE ' . $syslog_db_name . '.host_stats FIELDS ESCAPED BY \'\'';
			$sth = $self->db->prepare($query);
			$sth->execute($file);
			unlink($file);
		}
		else {
			$self->_process_upload($args, $file, $ret);
		}
	}
	catch {
		my $e = shift;
		$self->add_warning(500, $e);
		$self->log->error($e);
		$ret = $e;
	};
	
	$cb->($ret);
}	
		
sub _process_upload {
	my $self = shift;
	my $args = shift;
	my $file = shift;
	my $ret = shift;
	
	$self->log->debug("working on file $file, with existence: " . (-f $file));
	
	my ($query, $sth);
	my $syslog_db_name = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$syslog_db_name = $self->conf->get('syslog_db_name');
	}
	
	my $size = -s $file;
	
	if ($args->{description} or $args->{name}){
		# We're doing an import
		$args->{host} = $args->{client_ip_address};
		delete $args->{start};
		delete $args->{end};
		my $importer = new Import(log => $self->log, conf => $self->conf, db => $self->db, infile => $file, %$args);
		if (not $importer->id){
			throw(500, 'Import failed');
		}
		$ret->{import_id} = $importer->id;
		$self->log->info('Deleting successfully imported file ' . $file);
		unlink($file) if -f $file;
	}
	else {
		unless ($args->{start} and $args->{end}){
			my $msg = 'Did not receive valid start/end times';
			$self->log->error($msg);
			unlink($file);
			throw(400, $msg);
		}
		
		# Record our received file in the database
		$query = 'INSERT INTO ' . $syslog_db_name . '.buffers (filename, start, end) VALUES (?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($file, $args->{start}, $args->{end});
		$ret->{buffers_id} = $self->db->{mysql_insertid};
		
		$args->{batch_time} ||= 60;
		$args->{total_errors} ||= 0;
		
		# Record the upload
		$query = 'INSERT INTO ' . $syslog_db_name . '.uploads (client_ip, count, size, batch_time, errors, start, end, buffers_id) VALUES(?,?,?,?,?,?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{client_ip_address}, $args->{count}, $size, $args->{batch_time}, 
			$args->{total_errors}, $args->{start}, $args->{end}, $ret->{buffers_id});
		$ret->{upload_id} = $self->db->{mysql_insertid};
		$sth->finish;
	}
}

sub info {
	my $self = shift;
	my $args = shift;
	my $cb = shift;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer;
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my %results;
	my %stats;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { 
		my $overall_final = $self->_merge_node_info(\%results);
		$stats{overall} = (time() - $overall_start);
		$self->log->debug('stats: ' . Dumper(\%stats));
		$cb->($overall_final);
	});
	
	foreach my $peer (@peers){
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_info' : 'info';
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		# Do not proxy localhost requests
		my @no_proxy = ();
		if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
			push @no_proxy, proxy => undef;
		}
		$results{$peer} = http_get $url, headers => $headers, @no_proxy, sub {
			my ($body, $hdr) = @_;
			eval {
				my $raw_results = $self->json->decode($body);
				$stats{$peer}->{total_request_time} = (time() - $start);
				$results{$peer} = { %$raw_results }; #undef's the guard
			};
			if ($@){
				$self->log->error($@ . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $@, { http => $peer });
				delete $results{$peer};
			}
			$cv->end;
		};
	}
	$cv->end;
}

__PACKAGE__->meta->make_immutable;