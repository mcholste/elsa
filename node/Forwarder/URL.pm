package Forwarder::URL;
use Moose;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Request::Common;
use Digest::SHA qw(sha512_hex);

extends 'Forwarder';

has 'url' => (is => 'rw', isa => 'Str', required => 1);
has 'username' => (is => 'rw', isa => 'Str', required => 1);
has 'apikey' => (is => 'rw', isa => 'Str', required => 1);
has 'ua' => (is => 'rw', isa => 'LWP::UserAgent', required => 1);

sub identifiers { [ 'url' ] };

sub BUILDARGS {
	my ($class, %params) = @_;
	
	if (exists $params{verify_mode} and not $params{verify_mode}){
		$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;
	}
	else {
		$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 1;
	}
	
	$params{ua} = new LWP::UserAgent(agent => 'ELSA Log Relay/0.1', timeout => 10);
	$ENV{http_proxy} = $ENV{https_proxy} = $params{conf}->get('proxy') if $params{conf}->get('proxy');
	$params{ua}->env_proxy();
	
	my $timeout = $params{timeout} ? $params{timeout} : 60;
	my %ssl_opts = (Timeout => $timeout);
	foreach (qw(ca_file cert_file key_file verify_mode)){
		if (exists $params{$_}){
			$ssl_opts{'SSL_' . $_} = $params{$_};
		}
	}
	if (keys %ssl_opts){
		$params{ua}->ssl_opts(%ssl_opts);
	}
	
	return \%params;
}

sub _get_auth_header {
	my $self = shift;
	
	my $timestamp = CORE::time();
	return 'ApiKey ' . $self->username . ':' . $timestamp . ':' . sha512_hex($timestamp . $self->apikey);
}

sub forward {
	my $self = shift;
	my $args = shift;
	
	my $req;
	if ($args->{program_file}){
		$req = HTTP::Request::Common::POST($self->url,
			[
				program_file => $args->{program_file},
				filename => [ $args->{file} ],
				md5 => $args->{md5},
			],
			'Content_Type' => 'form-data',
			'Authorization' => $self->_get_auth_header());
	}
	else {
		$req = HTTP::Request::Common::POST($self->url,
			[
				md5 => $args->{md5},
				count => $args->{batch_counter},
				size => $args->{file_size},
				start => $args->{start},
				end => $args->{end},
				compressed => $args->{compressed} ? 1 : 0,
				batch_time => $args->{batch_time},
				total_errors => $args->{total_errors},
				filename => [ $args->{file} ],
				format => $args->{format},
				name => $args->{name},
				description => $args->{description},
				program => $args->{program},
			],
			'Content_Type' => 'form-data',
			'Authorization' => $self->_get_auth_header());
	}
	
	my $res = $self->ua->request($req);
	if ($res->is_success){
		my $ret = $res->content();
		$self->log->debug('got ret: ' . Dumper($ret));
		return 1;
	}
	else {
		$self->log->error('Failed to upload logs via url ' . $self->url . ': ' . $res->status_line);
		return 0;
	}				
}

__PACKAGE__->meta->make_immutable;

1;