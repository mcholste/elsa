package View::GoogleDatasource;
use Moose;
extends 'View';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Scalar::Util;
use Data::Google::Visualization::DataSource;
use Data::Google::Visualization::DataTable;
use DateTime;
use AnyEvent;
use Try::Tiny;
use Ouch qw(:trytiny);

with 'Fields';

sub call {
	my ($self, $env) = @_;
    
	my $req = Plack::Request->new($env);
	my $args = $req->parameters->as_hashref;
	my $datasource = Data::Google::Visualization::DataSource->new({
	    tqx => $args->{tqx},
	    xda => ($req->header('X-DataSource-Auth') || undef)
	});
	my $res = $req->new_response(200); # new Plack::Response
	my $ret;
	my $query_args;
	# If we don't have a nonblocking web server (Apache), we need to have an overarching blocking recv
	my $cv;
	if (not $env->{'psgi.nonblocking'}){
		$cv = AnyEvent->condvar;
	}
	
	return sub {
		my $write = shift;
		
		try {
			$self->session(Plack::Session->new($env));
			$res->content_type('text/plain');
			$res->header('Access-Control-Allow-Origin' => '*');
			
			$self->controller->clear_warnings;
			
			
			if ($self->session->get('user')){
				$args->{user} = $self->controller->get_stored_user($self->session->get('user'));
			}
			else {
				$args->{user} = $self->controller->get_user($req->user);
			}		
		
			my $check_args = $self->controller->json->decode($args->{q});
			if ($args->{user}->is_admin){
				$self->controller->log->debug('$check_args: ' . Dumper($check_args));
				# Trust admin
				$query_args = $check_args;
			}
			else {
				$query_args = $self->controller->_get_query($check_args) or die('Query not found'); # this is now from the database, so we can trust the input
			}
			$query_args->{auth} = $check_args->{auth};
			$query_args->{query_meta_params} = $check_args->{query_meta_params};
			$query_args->{user} = $args->{user};
			$query_args->{system} = 1;
			
			unless ($query_args->{uid} eq $args->{user}->uid or $args->{user}->is_admin){
				die('Invalid auth token') unless $self->controller->_check_auth_token($query_args);
				$self->controller->log->info('Running query created by ' . $query_args->{username} . ' on behalf of ' . $req->user);
				$query_args->{user} = $self->controller->get_user(delete $query_args->{username});
			}
			
			$self->controller->freshen_db;
			$self->controller->query($query_args, sub {
				my $ret = shift;
				unless ($ret){
					die($self->controller->last_error);
				}
			
				my $datatable = Data::Google::Visualization::DataTable->new();
				
				if (ref($ret) and $ret->{code}){
					throw($ret->{code}, $ret->{message}, $ret->{data});
				}
				elsif (blessed($ret) and $ret->groupby){
					#$self->controller->log->debug('ret: ' . Dumper($ret));
					$self->controller->log->debug('groupby: ' . Dumper($ret->groupby));
					
					# First add columns
					my $value_id = 0;
					my $groupby = $ret->groupby;
					$self->controller->log->debug('groupby: ' . Dumper($groupby));
					my $label = $ret->meta_params->{comment} ? $ret->meta_params->{comment} : 'count'; 
					if ($Fields::Time_values->{$groupby}){
						$datatable->add_columns({id => $groupby, label => $groupby, type => 'datetime'}, {id => 'value' . $value_id++, label => $label, type => 'number'});
					}
					else {
						if ($query_args->{query_meta_params}->{type} and $query_args->{query_meta_params}->{type} =~ /geo/i){
							$datatable->add_columns({id => $groupby, label => $groupby, type => 'string'}, {id => 'value' . $value_id++, label => $label, type => 'number'});
						}
						else {
							$datatable->add_columns({id => $groupby, label => $groupby, type => 'string'}, {id => 'value' . $value_id++, label => $label, type => 'number'});
						}
					}
					
					# Then add rows
					$label = $ret->meta_params->{comment} ? $ret->meta_params->{comment} : 'count'; 
					if ($Fields::Time_values->{$groupby}){
						my $tz = DateTime::TimeZone->new( name => "local");
						foreach my $row (@{ $ret->results->results->{$groupby} }){
							$self->controller->log->debug('row: ' . Dumper($row));
							$datatable->add_rows([ { v => DateTime->from_epoch(epoch => $row->{'intval'}, time_zone => $tz) }, { v => $row->{_count} } ]);
						}
					}
					else {
						if ($query_args->{query_meta_params}->{type} and $query_args->{query_meta_params}->{type} =~ /geo/i){
							# Hope for country code
							foreach my $row (@{ $ret->results->results->{$groupby} }){
								my $cc = $row->{_groupby};
								$self->controller->log->debug('row: ' . Dumper($row));
								if ($row->{_groupby} =~ /country=([^=]+)(\s\w+=|$)/i){
									$cc = $1;
								}
								elsif ($row->{_groupby} =~ /cc=(\w{2})/i){
									$cc = $1;
								}
								$datatable->add_rows([ { v => $cc }, { v => $row->{_count} } ]);
							}
						}
						else {
							foreach my $row (@{ $ret->results->results->{$groupby} }){
								$self->controller->log->debug('row: ' . Dumper($row));
								$datatable->add_rows([ { v => $row->{_groupby} }, { v => $row->{_count} } ]);
							}
						}
					}
				}
				elsif (blessed($ret)){
					throw(400, 'groupby required');
				}
				else {
					$self->controller->log->error('Unknown error with ret: ' . Dumper($ret));
					throw(500, 'Internal error');
				}
				$datasource->datatable($datatable);
				
				if (ref($ret) and ref($ret) eq 'HASH'){
					if ($self->controller->has_warnings){
						$self->controller->log->debug('warnings: ' . Dumper($self->controller->warnings));
						$datasource->add_message({type => 'warning', reason => 'data_truncated', message => join(' ', @{ $self->controller->warnings })});
					}
				}
				elsif (ref($ret) and blessed($ret) and $ret->can('add_warning') and $self->controller->has_warnings){
					$self->controller->log->debug('warnings: ' . Dumper($self->controller->warnings));
					$datasource->add_message({type => 'warning', reason => 'data_truncated', message => join(' ', @{ $self->controller->warnings })});
				}
				my ($headers, $body) = $datasource->serialize;
				$res->headers(@$headers);
				$res->body([encode_utf8($body)]);
				$self->controller->log->debug('headers: ' . Dumper(@$headers));
				$self->controller->log->debug('body: ' . Dumper($body));
				
				$write->($res->finalize());
				$cv and $cv->send;
			});
		}
		catch {
			my $e = shift;
			$self->controller->log->error($e);
			$datasource->add_message({type => 'error', reason => 'access_denied', message => $e});
			my ($headers, $body) = $datasource->serialize;
			$res->headers(@$headers);
			$res->body([encode_utf8($body)]);
			$write->($res->finalize());
			$cv and $cv->send;
		};
		$cv and $cv->recv;
	};
}

1;