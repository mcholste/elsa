#!/usr/bin/perl

# This script is used to forward scripts from a Novell Access Gateway into syslog and corresponds with the patterndb parser.
# Generally, a strategy of mounting the actualy access gateway log dirs onto the manager via NFS works the best, with
#  this script running on the manager.  Run this script in cron at any interval you like.  New files will be picked up 
#  at the interval.  The cron job will need a shell wrapper like this:
# #!/bin/sh
# pkill -f forward_novell_logs.pl
# /path/to/forward_novell_logs.pl

use strict;
use Data::Dumper;
use Log::Syslog::Fast ':all';
use IO::Handle;

my $base_dir = '/var/log/novell/reverse';
my $syslog_host = '127.0.0.1';
my $hostname = '127.0.0.1';

my @dirs;
opendir(DIR, $base_dir);
while (my $dir = readdir(DIR)){
	next if $dir =~ /^\./;
	push @dirs, $dir;
}
close(DIR);

foreach my $dir (@dirs){
	my $full_dir = $base_dir . "/" . $dir;
	my @files = qx(find $full_dir -type f);
	foreach (@files){ chomp; }
	my $files_str = join(' ', @files);

	my $pid = fork();
	if ($pid){
		print "Running in background for dir $dir.\n";
		exit;
	}
	else {

		my $logger = new Log::Syslog::Fast(LOG_UDP, $syslog_host, 514, LOG_LOCAL0, LOG_INFO, $hostname, 'novell_logs_' . $dir);
		my $pipe;
		my $tail_pid = open($pipe, "-|", "tail -q -f -n 0 $files_str");
		$pipe->autoflush(1);
		$pipe->blocking(1);

		local $SIG{TERM} = sub {
			system("kill $tail_pid");
			print "finished\n";
			exit;
		};

		eval {
			while (<$pipe>){
				chomp;
				my $current_time = time();
				$logger->send($_, $current_time);
				#print $_ . "\n";
			}
		};
	}
}
