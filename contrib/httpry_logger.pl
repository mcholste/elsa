#!/usr/bin/perl
use strict;
use Data::Dumper;
use Time::HiRes qw(time);
use Net::Server::Daemonize qw(daemonize);
use Getopt::Std;
use Socket qw(inet_aton inet_ntoa);
use Config::JSON;
use IO::File;
use Time::Local;

my %Opts;
getopts('hdDf:n:i:c:r:', \%Opts);

if ($Opts{h}){
	_usage() and exit;
}

my $Config = new Config::JSON($Opts{c}) or die('Unable to open config file ' . $Opts{c} . "\n" . _usage());

if (($Opts{D} or $Config->get('daemonize')) and !$Opts{r}){
	my $user = $Config->get('user') ? $Config->get('user') : 'root';
	my $group = $Config->get('group') ? $Config->get('group') : 'root';
	my $name = 'main';
	if ($Opts{n}){
		$name = $Opts{n};
	}
	elsif ($Config->get('name')){
		$name = $Config->get('name');
	}
		
	my $pid_file = $Config->get('pid_file') ? $Config->get('pid_file') : '/var/run/url_logger_' . $name . '.pid';
	print "Daemonizing...\n";
	daemonize($user, $group, $pid_file);
}
$| = 1;

my $Logger;
my $Sth;
my $File;
my $Mongo;
if ($Config->get('log/host')){
	require Log::Syslog::Fast;
	no strict 'subs'; # so perl doesn't complain about these constants
	$Logger = new Log::Syslog::Fast(
		Log::Syslog::Fast::LOG_UDP, 
		$Config->get('log/host'), 
		$Config->get('log/port'), 
		Log::Syslog::Fast::LOG_LOCAL0, 
		Log::Syslog::Fast::LOG_INFO, 
		$Config->get('log/from_host'), 
		$Config->get('log/program')) or die($!);
}
elsif ($Config->get('db')){
	require DBI;
	my $dbh = DBI->connect($Config->get('db/dsn'), $Config->get('db/username'), $Config->get('db/password')) or die($DBI::errstr);
	$Sth = $dbh->prepare($Config->get('db/insert_query')) or die($dbh->errstr);
}
elsif ($Config->get('file')){
	# Create the file if it doesn't exist.  Append if the file does.
	$File = new IO::File($Config->get('file'), O_WRONLY | O_APPEND | O_CREAT);
}
elsif ($Config->get('mongo')){
	require MongoDB;
	my $conn = MongoDB::Connection->new(
		host => $Config->get('mongo/host'), 
		port => $Config->get('mongo/port') 
	) or die('Unable to connect');
	my $db = $conn->get_database($Config->get('mongo/db'));
	$Mongo = $db->get_collection($Config->get('mongo/collection'));
}

my $Geoip;
if ($Config->get('geoip')){
	require Geo::IP; # get from Maxmind.com
	$Geoip = new Geo::IP(Geo::IP::GEOIP_MEMORY_CACHE()) or die('Unable to create GeoIP object: ' . $!);
}

my $Hostname_levels = 10;

my $Bpf = 'tcp';
if ($Opts{f}){
	$Bpf .= " && " . $Opts{f};
}
elsif ($Config->get('bpf')){
	$Bpf = $Config->get('bpf');
}

my $Interface = 'eth0';
if ($Opts{i}){
	$Interface = $Opts{i};
}
elsif ($Config->get('interface')){
	$Interface = $Config->get('interface');
}

my $Delimiter = '|';
if ($Config->get('delimiter')){
	$Delimiter = $Config->get('delimiter');
}

my %state;
my $Safety_limit = 20_000;
my $last_reported = time();
my $Timeout = 10;
my $responses = 0;
my $requests = 0;

my $Rfc_1918 = [
	[ unpack('N*', inet_aton('10.0.0.0')), unpack('N*', inet_aton('10.255.255.255')) ],
	[ unpack('N*', inet_aton('192.168.0.0')), unpack('N*', inet_aton('192.168.255.255')) ],
	[ unpack('N*', inet_aton('172.16.0.0')), unpack('N*', inet_aton('172.31.255.255')) ]
];

my $Run = 1;
if ($Opts{r}){
	$Run = 0;
}
do {
	eval {
		my $source = "-i $Interface";
		if ($Opts{r}){
			$source = "-r $Opts{r}";	
		}
		open(FH, "-|", "httpry -q $source -f timestamp,source-ip,x-forwarded-for,source-port,dest-ip,dest-port,method,host,request-uri,referer,user-agent,status-code,content-length,direction,cookie \"$Bpf\"");
		while (<FH>){
			chomp;
			my ($timestamp,$source_ip,$xff,$source_port,$dest_ip,$dest_port,$method,$host,$request_uri,$referer,$user_agent,$status_code,$content_length,$direction,$cookie) = split(/\t/, $_);
			# Use the X-Forwarded-For header if available
			if ($xff ne '-'){
				$source_ip = (split(/\s+/, $xff))[0];
			}
			my $tuple = "$source_ip:$source_port:$dest_ip:$dest_port";
			if ($direction eq '>'){
				my $domains = '';
				if ($host eq '-'){
					$host = $dest_ip;
				}
				else {
					my @hostname = split(/\./, $host);
					for (my $i = 1; $i < $Hostname_levels; $i++){
						last if $i > scalar @hostname;
						$domains .= ',' . join('.', @hostname[( -1 * $i )..-1 ]);
					}
				}
				# If we're overwriting a tuple, emit it so it's not lost
				if ($state{$tuple} and $state{$tuple}->{req}){
					emit($state{$tuple}->{req}, $state{$tuple}->{resp} ? $state{$tuple}->{resp} : {} );
				}
				$state{$tuple} = {};
				$request_uri =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;
				
				$state{$tuple}->{req} = { 
					timestamp => $timestamp,
					srcip => $source_ip, #store srcip/dstip here to avoid an expensive split on tuple later
					dstip => $dest_ip,
					srcport => $source_port,
					dstport => $dest_port,
					method => $method,
					host => $host, 
					request_uri => $request_uri, 
					referer => $referer, 
					user_agent => $user_agent,
					domains => $domains,
					cookie => $cookie,
				};
				if ($Opts{r}){
					my @date_parts = split(/[\-\ :]/, $timestamp);
					$state{$tuple}->{time} = timelocal($date_parts[5], $date_parts[4], $date_parts[3], $date_parts[2], ($date_parts[1] - 1), $date_parts[0]);
				}
				else {
					$state{$tuple}->{time} = time();
				}
				$requests++;
			}
			elsif ($direction eq '<'){
				# Swap source/dest because this is a response
				my $tmp = $source_ip;
				$source_ip = $dest_ip;
				$dest_ip = $tmp;
				$tmp = $source_port;
				$source_port = $dest_port;
				$dest_port = $tmp;
				$tuple = "$source_ip:$source_port:$dest_ip:$dest_port";
				
		 		#next if $status_code eq '-';
				$state{$tuple} ||= {};
				if ($content_length eq '-'){
					$content_length = '';
				}
				$state{$tuple}->{resp} = { status_code => $status_code, content_length => $content_length };
				if ($Opts{r}){
					my @date_parts = split(/[\-\ :]/, $timestamp);
					$state{$tuple}->{time} = timelocal($date_parts[5], $date_parts[4], $date_parts[3], $date_parts[2], ($date_parts[1] - 1), $date_parts[0]);
				}
				else {
					$state{$tuple}->{time} = time();
				}
				$responses++;
			}
			else {
				$Logger->error('Invalid line: ' . $_);
				next;
			}
			if ($state{$tuple}->{req} and $state{$tuple}->{resp}){
				emit($state{$tuple}->{req}, $state{$tuple}->{resp});
				delete $state{$tuple};
			}
			if ($. % 10_000 eq 0){
				#my $msg = 10_000 / (time() - $last_reported) . " req/sec ";
				#my $msg = 10_000 / ($timestamp - $last_reported) . " req/sec ";
				if ($Opts{r}){
					my @date_parts = split(/[\-\ :]/, $timestamp);
					$last_reported = timelocal($date_parts[5], $date_parts[4], $date_parts[3], $date_parts[2], ($date_parts[1] - 1), $date_parts[0]);
				}
				else {
					$last_reported = time();
				}
				my $msg = "state table size: " . (scalar keys %state) . " ";
				# State table maint
				my $deleted = 0;
				my $had_req = 0;
				my $had_resp = 0;
				foreach my $check_tuple (keys %state){
					if (not $state{$check_tuple}->{time} or ($state{$check_tuple}->{time} and (($last_reported - $state{$check_tuple}->{time}) > $Timeout)) ){
						my $expired = delete $state{$check_tuple};
						$deleted++;
						if ($expired->{req}){
							emit($expired->{req}, {});
							$had_req++;
						}
						if ($expired->{resp}){
							$had_resp++;
						}
					}
				}
				$msg .= "Purged $deleted sessions, $had_req had req, $had_resp had resp ";
				$msg .= "reqs: $requests, resps: $responses ";
				print $msg . "\n";
			}
			# Safety kill
			if (scalar keys %state > $Safety_limit){
				die("State table was greater than safety limit of $Safety_limit!");
			}
		}
	};
	if ($@){
		warn "$@";
		$Logger->send($@, time()) if $Logger;
	}
	# Reset
	%state = ();
	$last_reported = time();
	$Timeout = 10;
	$responses = 0;
	$requests = 0;
} while ($Run);

sub emit {
	my ($req, $resp) = @_;
	my ($source_ip, $dest_ip) = ($req->{srcip}, $req->{dstip});
	
	# Get the country code and convert it into char so we store it as an integer
	my @cc = unpack('c*', pack('A*', get_country_code($source_ip, $dest_ip)));
	my $country_code = join('', @cc);
	
	# Escape any pipes in URI/referer/UA
	$req->{request_uri} =~ s/[^a-zA-Z0-9\`\~\!\@\#\$\%\^\&\*\(\)\-\_\=\+\,\;\:\[\]\{\}\.\?\/\<\>\ \'\"]/\\\\/g;
	$req->{referer} =~ s/[^a-zA-Z0-9\`\~\!\@\#\$\%\^\&\*\(\)\-\_\=\+\,\;\:\[\]\{\}\.\?\/\<\>\ \'\"]/\\\\/g;
	$req->{user_agent} =~ s/[^a-zA-Z0-9\`\~\!\@\#\$\%\^\&\*\(\)\-\_\=\+\,\;\:\[\]\{\}\.\?\/\<\>\ \'\"]/\\\\/g;
	
	if ($Logger or $File){
		my $msg;
		if ($Config->get('log/include_cookie')){
			$msg = join($Delimiter,
				#$req->{timestamp}, 
				$source_ip, 
				$dest_ip,
				$req->{method},
				$req->{host}, 
				$req->{request_uri},
				$req->{referer},
				$req->{user_agent},
				$req->{domains},
				$resp->{status_code},
				$resp->{content_length},
				$country_code,
				$req->{cookie});
		}
		else {
			$msg = join($Delimiter,
				#$req->{timestamp}, 
				$source_ip, 
				$dest_ip,
				$req->{method},
				$req->{host}, 
				$req->{request_uri},
				$req->{referer},
				$req->{user_agent},
				$req->{domains},
				$resp->{status_code},
				$resp->{content_length},
				$country_code);
		}
		
		if ($Opts{d}){
			print $msg . "\n";
		}
		else {
			if ($Logger){
				$Logger->send($msg,	time());
			}
			if ($File){
				$File->print($msg . "\n");
			}
		}
	}
	if ($Sth){
		$Sth->execute($req->{timestamp},
			$source_ip, 
			$dest_ip,
			$req->{method},
			$req->{host}, 
			$req->{request_uri},
			$req->{referer},
			$req->{user_agent},
			$req->{domains},
			$resp->{status_code},
			$resp->{content_length},
			$country_code);
	}
	if ($Mongo){
		$Mongo->insert({
			srcip => unpack('N*', inet_aton($source_ip)),
			dstip => unpack('N*', inet_aton($dest_ip)),
			srcport => int($req->{srcport}),
			dstport => int($req->{dstport}),
			method => $req->{method},
			host => $req->{host},
			request_uri => $req->{request_uri},
			user_agent => $req->{user_agent},
			domains => [ split(/,/, $req->{domains}) ],
			status_code => int($resp->{status_code}),
			content_length => int($resp->{content_length}),
			country_code => int($country_code),
			cookie => $req->{cookie},
			uri_terms => [ split(/[^a-zA-Z0-9\-\_\.\@]+/, $req->{request_uri}) ],
			cookie_terms => [ split(/[^a-zA-Z0-9\-\_\.\@]+/, $req->{cookie}) ],
		});
	}
}

sub get_country_code {
	my $source_ip = shift;
	my $dest_ip = shift;
	
	return undef unless $Geoip;
	
	my $src_ip_int = unpack('N*', inet_aton($source_ip));
	my $is_rfc_1918 = 0;
	foreach my $ip_arr (@$Rfc_1918){
		if ($src_ip_int >= $ip_arr->[0] and $src_ip_int <= $ip_arr->[1]){
			$is_rfc_1918 = 1;
			last;
		}
	}
	
	# Use the other one if this one is 1918
	if ($is_rfc_1918){
		return $Geoip->country_code_by_addr($dest_ip);
	}
	else {
		# check the other
		my $dst_ip_int = unpack('N*', inet_aton($dest_ip));
		foreach my $ip_arr (@$Rfc_1918){
			if ($dst_ip_int >= $ip_arr->[0] and $dst_ip_int <= $ip_arr->[1]){
				$is_rfc_1918 = 1;
				last;
			}
		}
		if ($is_rfc_1918){
			return $Geoip->country_code_by_addr($source_ip);
		}
		else {
			# Neither are RFC 1918, so we pick the non-US one
			my $src_country_code = $Geoip->country_code_by_addr($source_ip);
			if ($src_country_code ne 'US'){
				return $src_country_code;
			}
			else {
				return $Geoip->country_code_by_addr($dest_ip);
			}
		}
	}
}

sub _usage {
	my $usage = <<EOT
-c <config file>
[ -D ] daemonize 
[ -f <BPF filter> ]
[ -i <interface> ]
[ -d ] Prints messages to STDOUT
[ -r <pcap dump file>] Read a pcap dump file instead of from an interface
[ -n <instance name> ] Name of the instance in case you are running multiple instances at the same time
EOT
;

	my $example_config = <<EOT
{
	"interface": "eth1",
	"daemonize": 1,
	"log": {
		"host": "syslog.example.com",
		"port": 514,
		"from_host": "httpry.example.com",
		"program": "url"
	},
	"db": {
		"dsn": "dbi:mysql:host=localhost:database=test",
		"username": "root",
		"password": "",
		"insert_query": "INSERT INTO httpry (timestamp, srcip, dstip, method, host, request_uri, referer, user_agent, domains, status_code, content_length) VALUES (?, INET_ATON(?), INET_ATON(?), ?, ?, ?, ?, ?, ?, ?, ?)"
	},
	"mongo": {
		"host": "localhost",
		"port": 27017,
		"db": "httpry",
		"collection": "httpry"
	},
	"file": "/var/log/urls.log"
}
EOT
;


	return $usage . "\nExample config:\n" . $example_config;
}

