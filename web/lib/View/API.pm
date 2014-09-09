package View::API;
use Moose;
extends 'View';
use Data::Dumper;
use Plack::Request;
use Encode;
use Scalar::Util;
use AnyEvent;
use Digest::SHA qw(sha512_hex);
use Try::Tiny;
use Ouch qw(:trytiny);

use Utils;

sub call {
	my ($self, $env) = @_;
    my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->controller->clear_warnings;
	
	Log::Log4perl::MDC->put('client_ip_address', $req->address);
	
	my $method = $self->extract_method($req->request_uri);
	$self->controller->log->debug('method: ' . $method);
	
	# Make sure private methods can't be run from the web
	if ($method =~ /^\_/){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	
	my $args = $req->parameters->as_hashref;
	if ($req->user_agent eq $self->controller->user_agent_name){
		$args->{from_peer} = $req->address;
	}
	else {
		$args->{from_peer} = '_external';
	}
	$args->{client_ip_address} = $req->address;
	
	$self->controller->log->debug('args: ' . Dumper($args));
	
	# Authenticate via apikey
	unless ($self->controller->_check_auth_header($req)){
		$res->status(401);
		$res->body('unauthorized');
		$res->header('WWW-Authenticate', 'ApiKey');
		return $res->finalize();
	}
	
	unless ($self->controller->can($method)){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	
	
	# If we don't have a nonblocking web server (Apache), we need to have an overarching blocking recv
	my $cv;
	if (not $env->{'psgi.nonblocking'}){
		$cv = AnyEvent->condvar;
	}
	
	return sub {
		my $write = shift;
		
		try {
			#$self->controller->freshen_db;
			if ($req->upload and $req->uploads->{filename}){
				$args->{upload} = $req->uploads->{filename};
			}
			$self->controller->$method($args, sub {
				my $ret = shift;
				if ($ret and ref($ret) eq 'Ouch'){
					$self->controller->log->error($ret->trace);
					$res->status($ret->code);
					$res->body([encode_utf8($self->controller->json->encode($ret))]);
				}
				elsif (ref($ret) and ref($ret) eq 'ARRAY'){
					# API function returned Plack-compatible response
					$write->($ret);
					$cv and $cv->send;
					return;
				}
				elsif (ref($ret) and ref($ret) eq 'HASH' and $ret->{mime_type}){
					$res->content_type($ret->{mime_type});
					
					if ($self->controller->has_warnings){
						$ret->{ret}->{warnings} = $self->controller->warnings;
					}
					
					$res->body($ret->{ret});
					if ($ret->{filename}){
						$res->header('Content-disposition', 'attachment; filename=' . $ret->{filename});
					}
				}
				else {
					if (ref($ret) and ref($ret) eq 'HASH'){
						if ($self->controller->has_warnings){
							$ret->{warnings} = $self->controller->warnings;
						}
					}
					elsif (ref($ret) and blessed($ret) and $ret->can('warnings') and $self->controller->has_warnings){
						foreach my $warning ($self->controller->all_warnings){
							push @{ $ret->warnings }, $warning;
						}
						if ($ret->can('dedupe_warnings')){
							$ret->dedupe_warnings();
						}
					}
					$res->body([encode_utf8($self->controller->json->pretty(0)->encode($ret))]);
				}
				
				$write->($res->finalize());
				#$write->([200, [ 'Content-Type' => 'text/plain' ], [$ret]]);
				$cv and $cv->send;
			});
		}
		catch {
			my $e = shift;
			ref($e) ? $self->controller->log->error($e->trace) : $self->controller->log->error($e);
			ref($e) ? $res->status($e->code) : $res->status(500);
			eval {
				$res->body([encode_utf8($self->controller->json->encode($e))]);
			};
			if ($@){
				$res->body([{ error => 'Internal error' }]);
			}
			$write->($res->finalize());
			$cv and $cv->send;
		};
		$cv and $cv->recv;
	};
}

__PACKAGE__->meta->make_immutable;
1;