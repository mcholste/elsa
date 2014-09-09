#!/usr/bin/perl
use strict;
use Data::Dumper;
use Time::HiRes qw(time);
use Getopt::Std;
use FindBin;
use lib $FindBin::Bin . '/lib';
use Controller;

my $config_file = -f '/etc/elsa_web.conf' ? '/etc/elsa_web.conf' : '/usr/local/elsa/etc/elsa_web.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}

my %opts;
getopts('f:q:c:', \%opts);
if ($opts{c}){
	$config_file = $opts{c};
}
die 'Invalid config file given' unless -f $config_file;
my $query = $opts{q};
unless (defined $query){
	$query = join(' ', @ARGV);
}
die usage() unless defined $query;

my $start = time();
my $api = API->new(config_file => $config_file);
my $user = User->new(conf => $api->conf, username => 'system');

my $q = $api->query({query_string => $query, user => $user, node_info => $api->info});
my $duration = time() - $start;
exit unless $q->results->total_records;
my $format = $opts{f} ? $opts{f} : 'tsv';
if ($q->has_warnings){
	foreach (@{ $q->warnings }){
		print "$_\n";
	}
}
print $api->format_results({ results => $q->results->results, format => $format }) . "\n";
print "Finished in $duration seconds.\n";

sub usage {
	return q{
usage: -q <query string>
       -c <web config file; default is /etc/elsa_web.conf, alternatively you can set env var ELSA_CONF>
       -f <format, default tsv; other choices are json and flat_json>
If called without arguments, all arguments are interpreted as the query and defaults are used.
};
}