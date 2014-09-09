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
use LWP::UserAgent;

my %Opts;
getopts('uhdDf:n:i:c:r:', \%Opts);

if ($Opts{h}){
	print _usage() and exit;
}

my $default_config = <<EOT
{
	"daemonize": 0,
	"log": {
		"host": "localhost",
		"port": 514,
		"from_host": "netflow_syslog",
		"program": "netflow_syslog"
	},
	"ralabel": {
		"binary": "ralabel",
		"config_file": "/etc/argus/ralabel.conf"
	},
	"asn": {
		"file": "/tmp/asn.tsv",
		"url": "http://bgp.potaroo.net/cidr/autnums.html",
		"tmp_dir": "/tmp",
		"local": 0
	}
}
EOT
;

my $Config = new Config::JSON($Opts{c}) or die('Unable to open config file ' . $Opts{c} . "\n" . _usage());

if (($Opts{D} or $Config->get('daemonize')) and !$Opts{r}){
	my $user = $Config->get('user') ? $Config->get('user') : getlogin;
	my $group = $Config->get('group') ? $Config->get('group') : getlogin;
	my $name = 'main';
	if ($Opts{n}){
		$name = $Opts{n};
	}
	elsif ($Config->get('name')){
		$name = $Config->get('name');
	}
		
	my $pid_file = $Config->get('pid_file') ? $Config->get('pid_file') : '/var/run/netflow_syslog_' . $name . '.pid';
	print "Daemonizing...\n";
	daemonize($user, $group, $pid_file);
}
$| = 1;

my $Logger;
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
else {
	die('No log config found in config file');
}

if ($Opts{u}){
	_get_arin_db();
}

my $ralabel = $Config->get('ralabel/binary') ? $Config->get('ralabel/binary') : 'ralabel';
my $ralabel_conf = $Config->get('ralabel/config_file') or die('no ralabel/config_file defined');
my $local_asn = $Config->get('asn/local') ? $Config->get('asn/local') : 0;

my $Delimiter = '|';
if ($Config->get('delimiter')){
	$Delimiter = $Config->get('delimiter');
}

my $print_only = 0;
if ($Opts{d}){
	$print_only = 1;
}

my $as_file = new IO::File($Config->get('asn/file')) or die('No ASN file found');
my %Asns;
while (<$as_file>){
	chomp;
	my ($asn, $desc) = split(/\t/, $_);
	$Asns{$asn} = $desc;
}
$as_file->close;

my $Run = 1;
if ($Opts{r}){
	$Run = 0;
}
do {
	eval {
		if ($Opts{r}){
			open(FH, "-|", "$ralabel -f $ralabel_conf -r $Opts{r}");
		}
		else {
			open(FH, "-|", "$ralabel -f $ralabel_conf");
		}
		while (<FH>){
			chomp;
			my @line = split(/\Q$Delimiter\E/, $_);
			my ($src, $dst) = split(/\:/, pop(@line));
			unless ($dst){
				$dst = $src;
				$src = undef;
			}
						
			if ($src and $line[6] eq $local_asn){
				splice(@line, 6, 1);
				(undef, $dst) = split(/=/, $dst);
				my @geo = split(/,/, $dst);
				push @line, join('', unpack('c*', pack('A*', uc($geo[0])))), (($geo[2] or $geo[1]) ? ($geo[2] . ', ' . $geo[1]) : undef), $geo[3], $geo[4];
				push @line, $Asns{ $line[7] };
			}
			elsif ($dst and $line[7] eq $local_asn){
				splice(@line, 7, 1);
				(undef, $src) = split(/=/, $src);
				my @geo = split(/,/, $src);
				push @line, join('', unpack('c*', pack('A*', uc($geo[0])))), (($geo[2] or $geo[1]) ? ($geo[2] . ', ' . $geo[1]) : undef), $geo[3], $geo[4];
				push @line, $Asns{ $line[6] };
			}
			elsif ($src and not $dst){
				splice(@line, 7, 1);
				(undef, $src) = split(/=/, $src);
				my @geo = split(/,/, $src);
				push @line, join('', unpack('c*', pack('A*', uc($geo[0])))), (($geo[2] or $geo[1]) ? ($geo[2] . ', ' . $geo[1]) : undef), $geo[3], $geo[4];
				push @line, $Asns{ $line[6] };
			}
			elsif ($dst and not $src){
				splice(@line, 6, 1);
				(undef, $dst) = split(/=/, $dst);
				my @geo = split(/,/, $dst);
				push @line, join('', unpack('c*', pack('A*', uc($geo[0])))), (($geo[2] or $geo[1]) ? ($geo[2] . ', ' . $geo[1]) : undef), $geo[3], $geo[4];
				push @line, $Asns{ $line[7] };
			}
			else {
				push @line, undef, undef, undef, undef, undef, undef;
			}
			
			if ($print_only){
				print join('|', @line) . "\n";
			}
			else {
				$Logger->send(join('|', @line),	time());
			}
		}
	};
	if ($@){
		warn "$@";
		$Logger->send($@, time()) if $Logger;
	}
	
} while ($Run);


sub _usage {
	my $usage = <<EOT
This scripts wraps the ralabel binary in the Argus Netflow distribution (http://www.qosient.com/argus/)
-c <config file>
[ -D ] Daemonize 
[ -d ] Prints messages to STDOUT
[ -r <dump file>] Read an Argus dump file instead of from the network
[ -n <instance name> ] Name of the instance in case you are running multiple instances at the same time
[ -u ] Update ASN database from web
EOT
;

	return $usage . "\nDefault config:\n" . $default_config;
}

sub _get_arin_db {
	my $ua = new LWP::UserAgent();
	my $file = $Config->get('asn/file');
	my $tmp_file = $Config->get('asn/tmp_dir') . '/asn.tmp';
	print "Updating ASN file from " . $Config->get('asn/url') . "...\n";
	$ua->mirror($Config->get('asn/url'), $tmp_file);
	
	die 'Unable to download file' unless -f $tmp_file;
	
	my $asn = new IO::File("> $file");
	$tmp_file = new IO::File($tmp_file);
	while (<$tmp_file>){
		chomp;
		if ($_ =~ /AS(\d+)\s+\<\/a\>\s+(.+)$/){
			$asn->print("$1\t$2\n");
		}
	}
	$asn->close;
	print "...done\n";
}

