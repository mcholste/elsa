#!/usr/bin/perl
use strict;
use Data::Dumper;
use Unix::PID;
use NetSNMP::agent qw(:all);
use NetSNMP::ASN qw(ASN_OCTET_STR ASN_INTEGER);
use DBI;
use Config::JSON;
use Sys::Hostname::FQDN;

#my $rootOID = ".1.3.6.1.4.1.8072.999";
#my $regoid = new NetSNMP::OID($rootOID);
my $hostname = Sys::Hostname::FQDN::short();
my $agent = new NetSNMP::agent();
$agent->register('check_pids', '.1.3.6.1.4.2010', \&handler_pids);
$agent->register('check_apps', '.1.3.6.1.4.2011', \&handler_apps);

my %Pidfiles = (
	'/data/sphinx/log/searchd.pid' => 'searchd',
	'/usr/local/syslog-ng/var/syslog-ng.pid' => 'syslog-ng',
	'/data/mysql/' . $hostname . '.pid' => 'mysql',
);

my $conf = new Config::JSON('/etc/elsa_node.conf');
my $Interval = 60;
my %DB_checks = (
	'elsa' => {
		db => $conf->get('meta_db'),
		query => 'SELECT COUNT(*) AS ok FROM query_log WHERE NOT ISNULL(milliseconds) AND timestamp > DATE_SUB(NOW(), INTERVAL ? SECOND)',
	}
);

my $upid = new Unix::PID();

sub handler_pids {
	my ($handler, $registration_info, $request_info, $requests) = @_;
	my $request;
	for ($request = $requests; $request; $request = $request->next()) {
		if ($request_info->getMode() == MODE_GET) {
			$request->setValue(ASN_INTEGER, check_pids());
		}
	}
}

sub check_pids {
	foreach my $pid_file (keys %Pidfiles){
		my $pid = $upid->get_pid_from_pidfile($pid_file) or return "Invalid pid file: $pid_file";
		my $info = $upid->pid_info_hash($pid);
		unless ($info->{COMMAND}){
			warn "No running process found for " . $Pidfiles{$pid_file};
			return 1;
		}
	}
	return 0;
}

sub handler_apps {
	my ($handler, $registration_info, $request_info, $requests) = @_;
	my $request;
	for ($request = $requests; $request; $request = $request->next()) {
		if ($request_info->getMode() == MODE_GET) {
			$request->setValue(ASN_INTEGER, check_apps());
		}
	}
}

sub check_apps {
	foreach my $check (keys %DB_checks){
		my $dbh = DBI->connect($DB_checks{$check}->{db}->{dsn}, $DB_checks{$check}->{db}->{username}, $DB_checks{$check}->{db}->{password}) or return 1;
		my $sth = $dbh->prepare($DB_checks{$check}->{query}) or die($dbh->errstr);
		$sth->execute($Interval);
		my $row = $sth->fetchrow_hashref or return 1;
		return 1 unless $row->{ok};
	}
	return 0;
}
