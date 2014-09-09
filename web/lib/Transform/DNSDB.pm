package Transform::DNSDB;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
use URL::Encode qw(url_encode);
use Time::HiRes;
extends 'Transform';

our $Name = 'DNSDB';
our $Limit = 100;
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'cv' => (is => 'rw', isa => 'Object');

#sub BUILDARGS {
#	my $class = shift;
#	my $params = $class->SUPER::BUILDARGS(@_);
#	$params->{cv} = AnyEvent->condvar;
#	return $params;
#}

sub BUILD {
	my $self = shift;
	
	# Use a configured limit if one exists
	if ($self->conf->get('transforms/dnsdb/limit')){
		$Limit = $self->conf->get('transforms/dnsdb/limit');
	}
	
	my $keys = {};
	if (scalar @{ $self->args }){
		foreach my $arg (@{ $self->args }){
			$keys->{$arg} = 1;
		}
	}
	else {
		$keys = { srcip => 1, dstip => 1, site => 1 };
	}	
	
	foreach my $datum (@{ $self->data }){
		$datum->{transforms}->{$Name} = {};
		
		$self->cv(AnyEvent->condvar);
		$self->cv->begin;
		foreach my $key (keys %{ $datum }){
			if ($datum->{$key} =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
				$datum->{transforms}->{$Name}->{$key} = {};
				$self->_query($datum, $key);
			}
		}
		
		$self->cv->end;
		$self->cv->recv;
	}
		
	return $self;
}

sub _query {
	my $self = shift;
	my $datum = shift;
	my $key = shift;
	my $query = $datum->{$key};
	
	$self->cv->begin;
	
	if ($query =~ /\d+\.\d+\.\d+\.\d+/){
		my $url = sprintf('https://api.dnsdb.info/lookup/rdata/ip/%s?limit=%d', $query, $Limit);
		
		my $info = $self->cache->get($url);
		if ($info and ref($info) eq 'HASH' and scalar keys %$info){
			$datum->{transforms}->{$Name}->{$key} = $info;
			$self->cv->end;
			return;
		}
		
		$self->log->debug('getting ' . $url);
		http_request GET => $url, headers => { 
			Accept => 'text/plain', 
			'X-API-Key' => $self->conf->get('transforms/dnsdb/apikey'), 
		}, 
		sub {
			my ($body, $hdr) = @_;
			$self->log->debug('got body ' . $body);
			my @lines = split(/\n/, $body);
			my $info = [];
			foreach my $line (@lines){
				my ($bailiwick) = split(/\s/, $line);
				next unless $bailiwick;
				chop($bailiwick);
				next if $bailiwick eq ';;';
				$bailiwick =~ /^[\w\.]+$/;
				push @$info, $bailiwick;
			}
			$datum->{transforms}->{$Name}->{$key} = { hostname => $info };
			#$info = { 'hostnames' => join(' ', @$info) };
			#$datum->{transforms}->{$Name}->{$key} = $info;
			$self->cache->set($url, { hostname => $info });
			$self->cv->end;
		};
	}
	else {
		my $url = sprintf('https://api.dnsdb.info/lookup/rrset/name/%s?limit=%d', $query, $Limit);
		
		my $info = $self->cache->get($url);
		if ($info){
			$datum->{transforms}->{$Name}->{$key} = $info;
			$self->cv->end;
			return;
		}
		
		$self->log->debug('getting ' . $url);
		http_request GET => $url, headers => { 
			Accept => 'application/json', 
			'X-API-Key' => $self->conf->get('transforms/dnsdb/apikey'),
		}, 
		sub {
			my ($body, $hdr) = @_;
			$self->log->debug('got body ' . $body);
			$self->cv->end and return unless $body;
			my @lines = split(/\n/, $body);
			my $uniq_ips = {};
			my $earliest = time();
			foreach my $line (@lines){
				next unless $line;
				my $datum = {};
				eval {
					$datum = decode_json($line);
					$self->log->trace('datum: ' . Dumper($datum));
				};
				if ($@){
					$self->log->error($line . ' ' . $url);
					next;
				}
				
				next unless $datum->{rrtype} eq 'A' or $datum->{rrtype} eq 'CNAME' or $datum->{rrtype} eq 'AAAA';
				if ($datum->{time_first} < $earliest){
					$earliest = $datum->{time_first};
				}
				foreach my $ip (@{ $datum->{rdata} }){
					$uniq_ips->{$ip} = 1;
				}	
			}
			my $info = { 
				earliest => scalar localtime($earliest), 
				ips => join(' ', keys %$uniq_ips), 
				age => sprintf('%.d days', ((time() - $earliest)/86400)),
			};
			$datum->{transforms}->{$Name}->{$key} = $info;
			$self->cache->set($url, $info);
			$self->cv->end;
		};
	};
	
}
 
1;
