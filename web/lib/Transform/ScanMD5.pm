package Transform::ScanMD5;
use Moose;
use Data::Dumper;
use LWP::UserAgent;
use JSON;
use Time::HiRes;
use AnyEvent;
use Try::Tiny;
use Ouch qw(:trytiny);
extends 'Transform';

our $Name = 'ScanMD5';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'lookups' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [ qw(VirusTotal ShadowServer) ] });
has 'cv' => (is => 'rw', isa => 'Object');

sub BUILD {
	my $self = shift;
	
	my %md5s;
	foreach my $record ($self->results->all_results){
		$record->{transforms}->{$Name} = {};
		if ($record->{msg} =~ / ([a-f0-9]{32}) /){
			$md5s{$1} ||= { ids => [], results => {} };
			push @{ $md5s{$1}->{ids} }, $record->{id};
		}  
	}
	
	$self->cv(AnyEvent->condvar); 
	$self->cv->begin(sub {
		$self->on_transform->($self->results);
	});
	
	try {
		foreach my $lookup_name (@{ $self->lookups }){
			my $lookup = "Transform::ScanMD5::$lookup_name"->new(log => $self->log, conf => $self->conf, cache => $self->cache);
			$lookup->query(\%md5s);
			foreach my $md5 (keys %md5s){
				next unless $md5s{$md5};
				$self->log->debug('$md5s{$md5} ' . Dumper($md5s{$md5}));
				foreach my $record ($self->results->all_results){
					$self->log->debug('$record: ' . Dumper($record));
					if (grep { $_ eq $record->{id} } @{ $md5s{$md5}->{ids} } and ref($md5s{$md5}->{results}) ){
						if ($record->{transforms}->{$Name}->{scan}){
							foreach my $av_name (keys %{ $md5s{$md5}->{results} }){
								$record->{transforms}->{$Name}->{scan}->{$av_name} = $md5s{$md5}->{results}->{$av_name};
							}
						}
						else {
							$record->{transforms}->{$Name}->{scan} = $md5s{$md5}->{results};
						}
					}
				}
			}
		}
		$self->cv->end;
	}
	catch {
		my $e = shift;
		$self->log->error('error in whois: ' . $e);
		$self->on_error->($e);
		$self->on_transform->($self->results);
	};
	
	return $self;
}

package Transform::ScanMD5::ShadowServer;
use Moose;
use Data::Dumper;
use LWP::UserAgent;
use JSON;
use Time::HiRes;
with 'Utils';

has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'ua' => (is => 'rw', isa => 'LWP::UserAgent', required => 1, default => sub { return LWP::UserAgent->new(); });
has 'url_template' => (is => 'ro', isa => 'Str', required => 1, default => 'http://innocuous.shadowserver.org/api/?query=%s');

sub query {
	my $self = shift;
	my $md5s = shift;
	
	foreach my $md5 (keys %$md5s){
		my $url = sprintf($self->url_template, $md5);
		if ($self->cache->get($url)){
			$self->log->trace('using cached result');
			$md5s->{$md5}->{results} = $self->cache->get($url);
		}
		$self->log->debug('Getting url ' . $url);
		my $response = $self->ua->get($url);
		if ($response->code eq 200){
			if ($response->decoded_content =~ /No match/){
				$self->log->debug('No result');
				$self->cache->set($url, 0);
				$md5s->{$md5}->{results} = 0;
				next;
			}
			elsif ($response->decoded_content =~ /Whitelisted/){
				$self->log->info('Whitelisted');
				$self->cache->set($url, 0);
				$md5s->{$md5}->{results} = 0;
				next;
			}
			$self->log->info('Got shadowserver.com result');
			my $ret;
			eval {
				my ($meta,$json) = split(/\n/, $response->decoded_content);
				my @meta_cols = qw(md5 sha1 first_date last_date type ssdeep);
				my %metas;
				@metas{@meta_cols} = split(/\,/, $meta);
				$ret = { meta => \%metas, results => decode_json($json) };
				$self->cache->set($url, $ret);
				$md5s->{$md5}->{results} = $ret;
			};
			if ($@){
				$self->log->error($@ . ': ' . $response->decoded_content);
			}
		}
		else {
			$self->log->debug('Communications failure: ' . Dumper($response));
			return 0;
		}
	}
}

package Transform::ScanMD5::VirusTotal;
use Moose;
use Data::Dumper;
use LWP::UserAgent;
use JSON;
use Time::HiRes;
with 'Utils';

has 'ua' => (is => 'rw', isa => 'LWP::UserAgent', required => 1, default => sub { return LWP::UserAgent->new(); });
has 'url' => (is => 'ro', isa => 'Str', required => 1, default => 'https://www.virustotal.com/vtapi/v2/file/report');
has 'apikey' => (is => 'ro', isa => 'Str', required => 1);
has 'json' => (is => 'rw', isa => 'Object', required => 1, default => sub { JSON->new->allow_blessed });
has 'throttle' => (is => 'rw', isa => 'Num', required => 1, default => 15); #https://www.virustotal.com/documentation/public-api/

sub BUILDARGS {
	my ($class, %params) = @_;
	$params{apikey} = $params{conf}->get('transforms/scanmd5/virustotal_apikey');
	return \%params;
}

sub query {
	my $self = shift;
	my $md5s = shift;
	
	$self->log->debug('looking up md5s: ' . Dumper($md5s));
	
	my @lookups = keys %$md5s;
	for (my $i = 0; $i < @lookups; $i += 4){
		$self->log->debug('Getting url ' . $self->url);
		my @resources;
		for (my $j = 0; $j < 4; $j++){
			last unless $lookups[$i + $j];
			push @resources, $lookups[$i + $j];
		}
		my $response = $self->ua->post($self->url, { resource => join(',', @resources), apikey => $self->apikey });
		if ($response->code eq 200){
			my $data = $self->json->decode($response->decoded_content);
			$self->log->debug('data: ' . Dumper($data));
			if (ref($data) ne 'ARRAY'){
				$data = [ $data ];
			}
			foreach my $datum (@$data){
				if ($datum->{positives}){
					my $result = { results => {}, ids => $md5s->{ $datum->{resource} } };
					foreach my $av_vendor (keys %{ $datum->{scans} }){
						next unless $datum->{scans}->{$av_vendor}->{result};
						$result->{results}->{$av_vendor} = $datum->{scans}->{$av_vendor}->{result};
					}
					$md5s->{ $datum->{resource} } = $result;
				}
				else {
					$md5s->{ $datum->{resource} } = { ids => [], results => undef };
				}
			}
		}
		else {
			$self->log->debug('Communications failure: ' . Dumper($response));
			return 0;
		}
		if ($i){
			$self->log->trace('sleeping for ' . $self->throttle);
			sleep $self->throttle;
		}
	}
	$self->log->debug('md5s: ' . Dumper($md5s));
}

 
1;