#!/usr/bin/perl
use strict;
use Data::Dumper;
use Socket;

my $s = '/data/elsa/tmp/debug';

if (-e $s){
	unlink($s) or die("Failed to unlink socket - check permissions.\n");
}

# be sure to set a correct umask so that the appender is allowed to stream to:
# umask(000);

socket(my $socket, PF_UNIX, SOCK_DGRAM, 0);
bind($socket, sockaddr_un($s)) or die("Error opening socket $s: $!");

while (1) {
	while (my $line = <$socket>) {
		print $line;
	}
}
