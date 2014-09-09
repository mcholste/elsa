#!/usr/bin/perl
use strict;
use DBI qw(:sql_types);
use Data::Dumper;
use Socket;
use Log::Syslog::Fast ':all';
my $elsa = '127.0.0.1';
my $odbc = '';
my $username = '';
my $password = '';
my $logger = Log::Syslog::Fast->new(LOG_UDP, $elsa, 514, LOG_LOCAL0, LOG_INFO, "mymachine", "symantec_antivirus");

my $dbh = DBI->connect("dbi:ODBC:$odbc", $username, $password, {RaiseError => 1}) or die($DBI::errstr);
my ($query, $sth);

$query = q{select source, noofviruses, filepath, actualaction, alertdatetime, clientuser1, virusname, 
sem_client.computer_name, sem_client.computer_domain_name, sem_client.user_name, sem_client.user_domain_name, sem_computer.ip_addr1
from alerts
join scans on alerts.computer_idx=scans.computer_idx
join virus on alerts.virusname_idx=virus.virusname_idx
join sem_client on alerts.computer_idx=sem_client.computer_id
join sem_computer on alerts.computer_idx=sem_computer.computer_id
join actualaction on alerts.actualaction_idx=actualaction.actualaction_idx
where virusname!='Tracking Cookies' AND alertdatetime > DATEADD(minute, -1, GETDATE())};
$query .= q{order by alertdatetime desc};
$sth = $dbh->prepare($query) or die($DBI::errstr);
$sth->execute();

my @rows;
while (my $row = $sth->fetchrow_hashref){
	$row->{ip} = inet_ntoa(pack('N*', delete $row->{ip_addr1}));
	$logger->send(sprintf('51: Security Risk Found!%s in File: %s by: %s. Action: %s Client:%s User:%s Host:%s', $row->{virusname}, $row->{filepath}, $row->{source}, $row->{actualaction}, $row->{ip}, $row->{user_name}, $row->{computer_name}), time);
}
