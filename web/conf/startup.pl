#!/usr/bin/env perl

use strict;
use warnings;
use Apache2::ServerUtil qw();

BEGIN {
	return unless Apache2::ServerUtil::restart_count() > 1;

	require lib;
	lib->import('/usr/local/elsa/web/lib');

	require Plack::Handler::Apache2;

	my @psgis = ('/usr/local/elsa/web/lib/Web.psgi');
	foreach my $psgi (@psgis) {
		Plack::Handler::Apache2->preload($psgi);
	}
}

1;    # file must return true!
