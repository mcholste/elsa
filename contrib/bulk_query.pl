#!/usr/bin/perl
use strict;
use Data::Dumper;
use Time::HiRes qw(time);
use Getopt::Std;
use FindBin;
use lib $FindBin::Bin . '/../web/lib', $FindBin::Bin . '/lib';
use API::Peers;
use LWP::UserAgent;
use Date::Manip;
my %Opts;
getopts('q:c:f:s:e:ht', \%Opts);

if ($Opts{h} or not $Opts{f}){
	print <<EOT
Usage:
perl bulk_query.pl -f <file containing terms> [ -c <config file> ] [ -s <start time> ] [ -e <end time> ] [ -t (format TSV) ] [ -q <query filter terms to add to each query> ]
EOT
;
}

my @terms;
open(FH, $Opts{f}) or die($!);
while (<FH>){
	chomp;
	push @terms, $_;
}
close(FH);

my $config_file = -f '/etc/elsa_web.conf' ? '/etc/elsa_web.conf' : '/usr/local/elsa/web/etc/elsa.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}
elsif ($Opts{c}){
	$config_file = $Opts{c};
}

my $start = time() - 86400;
if ($Opts{s}){
	$start = UnixDate(ParseDate($Opts{s}), '%s');
}
my $end = time() - 60;
if ($Opts{e}){
	$end = UnixDate(ParseDate($Opts{e}), '%s');
}

my $api = API::Peers->new(config_file => $config_file);

my $stats_start = time();
for (my $i = 0; $i < @terms; $i += 30){
	my $query_string = join(' ', @terms[$i..($i+30)]);
	if ($Opts{q}){
		$query_string .= ' ' . $Opts{q};
	}
	$query_string .= ' start:' . $start . ' end:' . $end;
	print $query_string . "\n";
	
	my $query = $api->query({ query_string => $query_string });
	my $duration = time() - $stats_start;
	next unless $query and $query->results->records_returned;
	my $format = $Opts{t} ? 'tsv' : 'json';
	if ($query->has_warnings){
		foreach (@{ $query->warnings }){
			print "$_\n";
		}
	}
	print $api->format_results({ format => $format, results => $query->results->results, groupby => $query->has_groupby ? $query->groupby : undef }) . "\n";
	print STDERR "Finished in $duration\n";
	print STDERR Dumper($query->stats);
}

