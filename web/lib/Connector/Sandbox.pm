package Connector::Sandbox;
use Moose;
use Data::Dumper;
use AnyEvent::HTTP;
use Date::Manip;
extends 'Connector';

our $Timeout = 10;
our $DefaultTimeOffset = 120;
our $Description = 'Send to malware analysis sandbox';
sub description { return $Description }
sub admin_required { return 1 }

sub BUILD {
	my $self = shift;
	
	# Get our ip/port/timestamp info from first entry
	my $record = $self->results->{results}->[0];
	$self->api->log->debug('timestamp: ' . $record->{timestamp});
	my $timestamp = $record->{timestamp};
	my $req_str = $self->api->conf->get('connectors/sandbox/url') . '?submit=executable&start=' . ($timestamp - $DefaultTimeOffset);
	$req_str .= '&end=' . ($timestamp + $DefaultTimeOffset);
	
	foreach my $field_hash (@{ $record->{_fields} }){
		if ($field_hash->{field} eq 'srcip'){
			$req_str .= '&srcip=' . $field_hash->{value};
		}
		elsif ($field_hash->{field} eq 'srcport'){
			$req_str .= '&srcport=' .  $field_hash->{value};
		}
		elsif ($field_hash->{field} eq 'dstip'){
			$req_str .= '&dstip=' .  $field_hash->{value};
		}
		elsif ($field_hash->{field} eq 'dstport'){
			$req_str .= '&dstport=' .  $field_hash->{value};
		}
	}
	
	
	$self->api->log->trace('sending to url ' . $req_str);
	my %headers = (timeout => $Timeout);
	# Apache won't use the /etc/hosts file for resolving names, so you need to set the site here as the name of
	#  the sandbox Apache vhost and use the IP in the URL to make the request. This is only necessary if 
	#  your site won't resolve using DNS.
	if ($self->api->conf->get('connectors/sandbox/site')){
		$headers{Host} = $self->api->conf->get('connectors/sandbox/site');
	}
	
	my $cv = AnyEvent->condvar;
	http_get($req_str,
		headers => \%headers,
		sub {
			my ($body, $hdr) = @_;
			if ($!){
				$self->api->log->error('error: ' . $!);
			}
			$self->api->log->trace('body: ' . Dumper($body));
			$cv->send;
		}
	);
	$cv->recv;
	return 1;
}


1