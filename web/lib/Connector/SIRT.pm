package Connector::SIRT;
use Moose;
use Data::Dumper;
use AnyEvent::HTTP;
use MIME::Base64;
use Digest::MD5;
use URI::Escape;
extends 'Connector';

our $Description = 'Send to SIRT';
sub description { return $Description }
sub admin_required { return 1 }

sub BUILD {
	my $self = shift;
	
	$self->api->log->trace('posting to url ' . $self->api->conf->get('connectors/sirt/url'));
	my $post_str = 'data=' . uri_escape(encode_base64($self->api->json->encode( { data => $self->results->{results}, query => $self->results->{query_string} } )));
	$post_str .= '&username=' . $self->user->username;
	#$self->api->log->trace('post_str: ' . $post_str);
	my $cv = AnyEvent->condvar;
	http_post(
		$self->api->conf->get('connectors/sirt/url'), 
		$post_str, 
		headers => { 'Content-type' => 'application/x-www-form-urlencoded' }, 
		sub {
			my ($body, $hdr) = @_;
			if ($!){
				$self->api->log->error('error: ' . $!);
			}
			#$self->api->log->trace('body: ' . Dumper($body));
			$cv->send;
		}
	);
	$cv->recv;
	return 1;
}


1