package View;
use Moose;
use base 'Plack::Component';
with 'WebUtils';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use JSON -convert_blessed_universally;
use URI::Escape qw(uri_unescape);
use Encode;
use MIME::Base64;
use Try::Tiny;
use Ouch qw(:trytiny);
use AnyEvent;

use Utils;
use YUI;
use Query;
use Controller;

has 'mode' => (is => 'rw', isa => 'Str', required => 1, default => sub { return 'index' });
has 'session' => (is => 'rw', isa => 'Object', required => 0);
has 'controller' => (is => 'rw', isa => 'Object', required => 1);
has 'title' => (is => 'rw', isa => 'Str', required => 1, default => 'ELSA');
has 'path_to_inc' => (is => 'rw', isa => 'Str', required => 1, default => '');

our %Modes = (
	index => 1,
	chart => 1,
	stats => 1,
	get_results => 1,
	admin => 1,
	#transform => 2,
	send_to => 1,
);

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/html');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->controller->clear_warnings;
	
	Log::Log4perl::MDC->put('client_ip_address', $req->address);
	
	# If we don't have a nonblocking web server (Apache), we need to have an overarching blocking recv
	my $cv;
	if (not $env->{'psgi.nonblocking'}){
		$cv = AnyEvent->condvar;
	}
	return sub {
		my $write = shift;
		try {
			my $body;
			my $method = $self->extract_method($req->request_uri);
			$method ||= 'index';
			$self->controller->log->debug('method: ' . $method);
			if (exists $Modes{ $method }){
				if ($Modes{ $method } == 1){
					my $user = $self->controller->get_user($req->user);
					if ($user){
						$self->session->set('user', $user->freeze);
						$self->session->set('user_info', $user->TO_JSON);
						$self->$method($req, $user, sub {
							$body = shift;
							if (ref($body) and ref($body) eq 'HASH'){
								if ($self->controller->has_warnings){
									$body->{warnings} = $self->controller->warnings;
								}
								$body = [encode_utf8($self->controller->json->encode($body))];
								$self->controller->log->trace('returning body: ' . Dumper($body));
							}
							$res->body($body);
							$write->($res->finalize());
							$cv and $cv->send;
						});
					}
					else {
						$res->status(401);
						$body = 'Unauthorized';
						$res->body($body);
						$write->($res->finalize());
						$cv and $cv->send;
					}
				}
				elsif ($Modes{ $method } == 2){
					$self->$method($req, sub {
						my $ret = shift;
						if (not $ret){
							$ret = { error => $self->controller->errors };
							$body = [encode_utf8($self->controller->json->encode($ret))];
						}
						elsif (ref($ret) and $ret->{mime_type}){
							$res->content_type($ret->{mime_type});
							$body = $ret->{ret};
							if ($ret->{filename}){
								$res->header('Content-disposition', 'attachment; filename=' . $ret->{filename});
							}
						}
						else {
							$body = [encode_utf8($self->controller->json->encode($ret))];
						}
						$res->body($body);
						$write->($res->finalize());
						$cv and $cv->send;
					});
				}
			}
			else {
				my $user = $self->controller->get_user($req->user);
				my $args = $req->parameters->as_hashref;
				$self->controller->log->debug('args: ' . Dumper($args));
				if ($user){
					if (not $self->controller->can($method)){
						$res->status(404);
						$body = 'Not found';
						$res->body($body);
						$write->($res->finalize());
						$cv and $cv->send;
						return;
					}
					$self->session->set('user', $user->freeze);
					$self->session->set('user_info', $user->TO_JSON);
					$args->{user} = $user;
					
					$self->controller->$method($args, sub {
						my $ret = shift;
						if (not $ret){
							$ret = { error => $self->controller->errors };
							$body = [encode_utf8($self->controller->json->encode($ret))];
						}
						elsif (ref($ret) and $ret->{mime_type}){
							$res->content_type($ret->{mime_type});
							$body = $ret->{ret};
							if ($ret->{filename}){
								$res->header('Content-disposition', 'attachment; filename=' . $ret->{filename});
							}
						}
						else {
							$body = [encode_utf8($self->controller->json->encode($ret))];
						}
						$res->body($body);
						$write->($res->finalize());
						$cv and $cv->send;
					});
				}
				else {
					$res->status(401);
					$body = 'Unauthorized';
					$res->body($body);
					$write->($res->finalize());
					$cv and $cv->send;
				}
			}
		}
		catch {
			my $e = shift;
			$self->controller->log->error($e);
			$res->body([encode_utf8($self->controller->json->encode({error => $e}))]);
			$write->($res->finalize());
			$cv and $cv->send;
		};
		$cv and $cv->recv;
	};
}


sub index {
	my $self = shift;
	my $req = shift;
	my $user = shift;
	my $cb = shift;
	
	$self->title('ELSA');
	
	return $self->get_headers(sub {
		my $headers = shift;
		$self->get_index_body(sub {
			my $body = shift;
			$cb->($headers . $body);
		});
	});
}

sub get_results {
	my $self = shift;
	my $req = shift;
	my $user = shift;
	my $cb = shift;
	
	$self->get_headers(sub {
		my $HTML = shift;
		my $body_html = <<'EOHTML'
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content">
	    </div>
    </div>
</div>
</body>
</html>
EOHTML
;
		if ($self->session->get('user_info') and $self->session->get('user_info')->{uid}){
			my $args = $req->query_parameters->as_hashref;
			$args->{uid} = $self->session->get('user_info')->{uid};
			
					
			$self->controller->get_saved_result($args, sub {
				my $ret = shift;
				if ($ret and ref($ret) eq 'HASH'){
					my $user = $self->controller->get_user($self->session->get('user_info')->{username});
					$HTML .= '<script>var oGivenResults = ' . $self->controller->json->encode($ret) . '</script>';
					$HTML .= '<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Results.Given(oGivenResults)});</script>';
					$HTML .= $body_html;

					$cb->($HTML);
				}
				else {
					$self->controller->_error('Unable to get results, got: ' . Dumper($ret));
					$HTML .= '<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Error("Unable to get results"); });</script>';
					$cb->($HTML);
				}
			});
		}
		else {
			$self->controller->_error('Unauthorized');
			$HTML .= '<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Error("Unauthorized"); });</script>';
			$HTML .= $body_html;
			$cb->($HTML);
		}
	});
}

sub admin {
	my $self = shift;
	my $req = shift;
	my $user = shift;
	my $cb = shift;
	
	$self->title('ELSA Permissions Management');
	
	my $args = $req->query_parameters->as_hashref;
	$args->{uid} = $self->session->get('user_info')->{uid};
	$self->get_headers(sub {
		my $HTML = shift;
		$HTML .= <<'EOHTML'
<script type="text/javascript" src="inc/admin.js" ></script>
<script>YAHOO.util.Event.addListener(window, "load", YAHOO.ELSA.Admin.main)</script>
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="permissions"></div>
<div id="delete_exceptions_button_container"></div>
<div id="exceptions"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content">
	    </div>
    </div>
</div>
</body>
</html>
EOHTML
;
	
		$cb->($HTML);
	});
}

sub stats {
	my $self = shift;
	my $req = shift;
	my $user = shift;
	my $cb = shift;
	
	$self->title('ELSA Stats');
	
	my $args = $req->query_parameters->as_hashref;
	$args->{uid} = $self->session->get('user_info')->{uid};
	$self->get_headers(sub {
		my $HTML = shift;
		$HTML .= <<'EOHTML'
<script type="text/javascript" src="inc/stats.js" ></script>
<script>YAHOO.util.Event.addListener(window, "load", YAHOO.ELSA.Stats.main)</script>
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="query_stats"></div>
<div id="load_stats"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content">
	    </div>
    </div>
</div>
</body>
</html>
EOHTML
;
	
		$cb->($HTML);
	});
}

#sub transform {
#	my $self = shift;
#	my $req = shift;
#	my $cb = shift;
#	my $args = $req->parameters->as_hashref;
#	
#	$self->title('ELSA Transform');
#	
#	if ( $args and ref($args) eq 'HASH' and $args->{data} and $args->{transforms} ) {
#		try {
#			$self->controller->log->trace('args: ' . Dumper($args));
#			$args->{transforms} = $self->controller->json->decode(uri_unescape($args->{transforms}));
#			$self->controller->log->trace('transforms: ' . Dumper($args->{transforms}));
#			foreach my $transform (@{ $args->{transforms} }){
#				throw(400, 'subsearch not allowed', { transform => 'subsearch' }) if $transform eq 'subsearch';
#			}
#			$args->{results} = $self->controller->json->decode(uri_unescape(delete $args->{data}));
#			$self->controller->log->debug( "Decoded $args as : " . Dumper($args) );
#		}
#		catch {
#			my $e = catch_any($_);
#			$self->controller->log->error("invalid args, error: $e, args: " . Dumper($args));
#			#return { error => 'Unable to build results object from args' };
#			$e->throw;
#		}
#		
#		my $res = new Results(results => (ref($args->{results}) eq 'ARRAY' ? $args->{results} : $args->{results}->{results}));
#		$self->controller->log->debug('res: ' . Dumper($res));
#		my $q = new Query(conf => $self->controller->conf, results => $res, transforms => $args->{transforms});
#		$self->controller->transform($q);
#		my $results = $q->results->results;
#		
#		$self->controller->log->debug( "Got results: " . Dumper($results) );
#		
#		return { 
#			ret => $results, 
#			mime_type => 'application/javascript',
#		};
#	}
#	else {
#		$self->controller->log->error('Invalid args: ' . Dumper($args));
#		return { error => 'Unable to build results object from args' };
#	}
#}

sub send_to {
	my $self = shift;
	my $req = shift;
	my $user = shift;
	my $cb = shift;
	my $args = $req->parameters->as_hashref;
	
	$self->title('ELSA Connector');
	
	if ( $args and ref($args) eq 'HASH' and $args->{data} ) {
		eval {
			my $json_args = $self->controller->json->decode(uri_unescape(decode_base64($args->{data})));
			$args->{user} = $self->controller->get_user($req->user);
			$args->{connectors} = $json_args->{connectors};
			$args->{results} = delete $json_args->{results};
			$args->{query} = delete $json_args->{query};
			$args->{qid} = delete $json_args->{qid};
			$self->controller->log->debug( "Decoded $args as : " . Dumper($args) );
		};
		if ($@){
			$self->controller->log->error("invalid args, error: $@, args: " . Dumper($args));
			return 'Unable to build results object from args';
		}
		
		$self->controller->send_to($args, sub {
			my $results = shift;
			$results = $args->{results} unless $results;
			$self->controller->log->debug( "Got results: " . Dumper($results) );
			
			$cb->({ 
				ret => $results, 
				mime_type => 'application/javascript',
			});
		});
	}
	else {
		$self->controller->log->error('Invalid args: ' . Dumper($args));
		throw(400, 'Unable to build results object from args');
	}
}

1;