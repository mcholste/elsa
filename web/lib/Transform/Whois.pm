package Transform::Whois;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
use Try::Tiny;
use Ouch qw(:trytiny);;
extends 'Transform';

use Utils;

our $Name = 'Whois';
our $Timeout = 3; # We're going to need to fail quickly
our %Proxy;

# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'cv' => (is => 'rw', isa => 'Object');
has 'cache_stats' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { { hits => 0, misses => 0 } });
has 'lookups' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

#sub BUILDARGS {
#	my $class = shift;
#	my $params = $class->SUPER::BUILDARGS(@_);
#	$params->{cv} = AnyEvent->condvar;
#	return $params;
#}

sub BUILD {
	my $self = shift;
	
	if ($self->conf->get('proxy') and $self->conf->get('proxy') =~ /(http[s]?):\/\/([^:]+):(\d+)/){
		%Proxy = (proxy => [ $2, $3, $1 ]);
	}
	
	my $keys = {};
	if (scalar @{ $self->args }){
		foreach my $arg (@{ $self->args }){
			$keys->{$arg} = 1;
		}
	}
	else {
		$keys = { srcip => 1, dstip => 1, site => 1, ip => 1 };
	}
	
	# First find all the unique lookups we'll need
	foreach my $record ($self->results->all_results){
		foreach my $key ($self->results->keys($record)){
			next unless $key eq '_groupby' or exists $keys->{$key};
			my $display_key = $key;
			if ($key eq '_groupby'){
				$display_key = ($self->results->all_groupbys)[0];
			}
			my $value = $self->results->value($record, $key);
			if ($value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
				$self->lookups->{$value} ||= [];
				push @{ $self->lookups->{$value} }, { key => $display_key, record => $record };
			}
		}
	}
	$self->log->debug('lookups: ' . Dumper($self->lookups));
	
	$self->cv(AnyEvent->condvar); 
	$self->cv->begin(sub {
		$self->on_transform->($self->results);
	});
	
	try {
		# Perform lookups
		foreach my $key (sort keys %{ $self->lookups }){
			$self->_lookup($key);
		}
		
		foreach my $record ($self->results->all_results){			
			foreach my $key (qw(srcip dstip)){
				if ($record->{transforms}->{$Name}->{$key} and $record->{transforms}->{$Name}->{$key}->{is_local}){
					my $deleted = delete $record->{transforms}->{$Name}->{$key};
					$record->{transforms}->{$Name}->{$key} = { customer => $deleted->{customer}, ip => $deleted->{ip} };
					last;
				}
			}
		}
		$self->log->info('cache hits: ' . $self->cache_stats->{hits} . ', misses: ' . $self->cache_stats->{misses});
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

sub _update_records {
	my $self = shift;
	my $subject = shift;
	my $value = shift;
	
	return unless exists $self->lookups->{$subject};
	
	foreach my $to_update (@{ $self->lookups->{$subject} }){
		if (exists $to_update->{record}->{transforms}->{$Name}->{ $to_update->{key} }){
			if (ref($value) and ref($value) eq 'HASH'){
				foreach my $key (keys %$value){
					$to_update->{record}->{transforms}->{$Name}->{ $to_update->{key} }->{$key} = $value->{$key};
				}
			}
			else {
				$self->log->error('Value to update was not a hash');
				$to_update->{record}->{transforms}->{$Name}->{ $to_update->{key} } = $value;
			}
		}
		else {
			$to_update->{record}->{transforms}->{$Name}->{ $to_update->{key} } = $value;
		}
		$self->log->debug('$to_update: ' . Dumper($to_update));
		$self->log->debug('record is now: ' . Dumper($to_update->{record}));
	}
}
sub _lookup {
	my $self = shift;
	my $ip = shift;
	
	my $ret = { ip => $ip };
	$self->log->trace('Looking up ip ' . $ip);
	$self->cv->begin;
	
	# Check known orgs
	my $ip_int = unpack('N*', inet_aton($ip));
	if ($self->conf->get('transforms/whois/known_subnets') and $self->conf->get('transforms/whois/known_orgs')){
		my $known_subnets = $self->conf->get('transforms/whois/known_subnets');
		my $known_orgs = $self->conf->get('transforms/whois/known_orgs');
		foreach my $start (keys %$known_subnets){
			my $start_int = unpack('N*', inet_aton($start));
			if ($start_int <= $ip_int and unpack('N*', inet_aton($known_subnets->{$start}->{end})) >= $ip_int){
				$ret->{customer} = $known_subnets->{$start}->{org};
				foreach my $key (qw(name descr org cc country state city)){
					$ret->{$key} = $known_orgs->{ $known_subnets->{$start}->{org} }->{$key};
				}
				$self->log->trace('using local org');
				$ret->{is_local} = 1;
				$self->_update_records($ip, $ret);
				$self->cv->end;
				return;
			}
		}
	}
	my $ip_url = 'http://whois.arin.net/rest/ip/' . $ip;
	my $ip_info = $self->cache->get($ip_url, expire_if => sub {
		if ($_[0]->value->{name} or $_[0]->value->{ripe_ip}){
			return 0;
		}
		return 1;
	});
	if ($ip_info and $ip_info->{name}){
		$self->log->trace( 'Using cached ip ' . Dumper($ip_info) );
		$self->cache_stats->{hits}++;
		$ret->{name} = $ip_info->{name};
		$ret->{descr} = $ip_info->{descr};
		$ret->{org} = $ip_info->{org};
		$ret->{cc} = $ip_info->{cc} ? $ip_info->{cc} : 'US';
		my $org_url = $ip_info->{org_url};
		unless ($org_url){
			$self->log->warn('No org_url found from ip_url ' . $ip_url . ' in ip_info: ' . Dumper($ip_info));
			$self->cv->end;
			return;
		}
		
		my $org = $self->cache->get($org_url);
		if ($org){
			$self->log->trace( 'Using cached org' );
		}
		else {
			$org = $self->_lookup_org($org_url);
		}
		
		foreach my $key (keys %$org){
			$ret->{$key} = $org->{$key} if defined $org->{$key};
		}
		$self->_update_records($ip, $ret);
		
		$self->cv->end;
		return;
	}
	elsif ($ip_info and $ip_info->{ripe_ip}){
		$self->cache_stats->{hits}++;
		$self->_lookup_ip_ripe($ip_info->{ripe_ip}, $ip);
		return;
	}
	else {
		$self->log->debug('got cached ip_info: ' . Dumper($ip_info));
	}
	
	$self->log->debug( 'getting ' . $ip_url );
	$self->cache_stats->{misses}++;
	
	http_request GET => $ip_url, timeout => $Timeout, %Proxy, headers => { Accept => 'application/json' }, sub {
		my ($body, $hdr) = @_;
		my $whois;
		eval {
			$whois = decode_json($body);
		};
		if ($@){
			$self->log->error('Error getting ' . $ip_url . ': ' . $@);
			$self->cv->end;
			return;
		}
		$self->log->trace( 'got whois: ' . Dumper($whois) );
		if ($whois->{net}->{orgRef}){
			if ($whois->{net}->{orgRef}->{'@name'}){
				if ($whois->{net}->{orgRef}->{'@handle'} eq 'RIPE'
					or $whois->{net}->{orgRef}->{'@handle'} eq 'APNIC'
					or $whois->{net}->{orgRef}->{'@handle'} eq 'AFRINIC'
					or $whois->{net}->{orgRef}->{'@handle'} eq 'LACNIC'){
					$self->log->trace('Getting RIPE IP with org ' . $whois->{net}->{orgRef}->{'@handle'});
					$self->cache->set($ip_url, { ripe_ip => $whois->{net}->{orgRef}->{'@handle'} });
					$self->_update_records($ip, $ret); # set ip for the record
					$self->_lookup_ip_ripe($whois->{net}->{orgRef}->{'@handle'}, $ip);
					return;
				}
				else {
					$ret->{name} = $whois->{net}->{name}->{'$'};
					$ret->{descr} = $whois->{net}->{orgRef}->{'@name'};
					$ret->{org} = $whois->{net}->{orgRef}->{'@handle'};
					
					my $org_url = $whois->{net}->{orgRef}->{'$'};
					$self->log->debug( 'set cache for ' . $ip_url );
					#TODO set the cache for the subnet, not the IP so we can avoid future lookups to the same subnet
					$self->cache->set($ip_url, {
						name => $ret->{name},
						descr => $ret->{descr},
						org => $ret->{org},
						org_url => $org_url,
					});
					
					my $org = $self->cache->get($org_url);
					if ($org){
						$self->log->trace( 'Using cached org' );
						foreach my $key (keys %$org){
							$ret->{$key} = $org->{$key} if defined $org->{$key};
						}
						$self->_update_records($ip, $ret);
						$self->cv->end;
					}
					else {
						$self->_update_records($ip, $ret);
						$self->_lookup_org($org_url, $ip);
					}
				}
			}
		}
		elsif ($whois->{net}->{customerRef} and $whois->{net}->{customerRef}->{'$'}){
			my $org;
			$ret->{org} = $whois->{net}->{customerRef}->{'@handle'};
			$ret->{descr} = $whois->{net}->{customerRef}->{'@name'};
			$ret->{name} = $whois->{net}->{name}->{'$'};
			
			my $org_url = $whois->{net}->{customerRef}->{'$'};
			$self->log->debug( 'set cache for ' . $ip_url );
			#TODO set the cache for the subnet, not the IP so we can avoid future lookups to the same subnet
			$self->cache->set($ip_url, {
				name => $ret->{name},
				descr => $ret->{descr},
				org => $ret->{org},
				org_url => $org_url,
			});
			$self->cache_stats->{misses}++;
			
			$org = $self->cache->get($org_url);
			if ($org){
				$self->log->trace( 'Using cached org' );
				$self->cache_stats->{hits}++;
				foreach my $key (keys %$org){
					$ret->{$key} = $org->{$key} if defined $org->{$key};
				}
				$self->_update_records($ip, $ret);
				$self->cv->end;
			}
			else {
				$org = $self->_lookup_org($org_url, $ip);
			}
		}
		else {
			$self->log->error('Did not get org or customer ref: ' . Dumper($whois));
			$self->cv->end;
		}
		return;
	};
	$self->log->debug( 'sent ' . $ip_url );
}

sub _lookup_ip_ripe {
	my $self = shift;
	my $registrar = shift;
	my $ip = shift;
	
	my $ret = {};
	
	#my $ripe_url = 'http://apps.db.ripe.net/whois/grs-lookup/' . lc($registrar) . '-grs/inetnum/' . $ip;
	my $ripe_url = 'http://rest.db.ripe.net/search?query-string=' . $ip;
	my $cached = $self->cache->get($ripe_url);
	if ($cached){
		$self->log->trace('Using cached url ' . $ripe_url); 
		$self->cache_stats->{hits}++;
		foreach my $key (keys %$cached){
			$ret->{$key} = $cached->{$key} if defined $cached->{$key};
		}
		$self->_update_records($ip, $ret);
		$self->log->trace('end');
		$self->cv->end;
		return;
	}
	
	$self->log->trace('Getting ' . $ripe_url);
	$self->cache_stats->{misses}++;
	http_request GET => $ripe_url, timeout => $Timeout, %Proxy, headers => { Accept => 'application/json' }, sub {
		my ($body, $hdr) = @_;
		# Clean up the invalid JSON (key names are the same due to invalid XML->JSON conversion at RIPE)
		my $counter = 0;
		while ($body =~ /"object":/){
			$body =~ s/"object":/"o$counter":/;
			$counter++;
		}
		my $whois;
		eval {
			$whois = decode_json($body);
		};
		if ($@){
			$self->log->error($body . ' ' . $ripe_url);
			$self->cv->end;
			return;
		}
#		if ($whois->{'whois-resources'} 
#			and $whois->{'whois-resources'}->{objects}
#			and $whois->{'whois-resources'}->{objects}->{object}
#			and $whois->{'whois-resources'}->{objects}->{object}->{attributes}
#			and $whois->{'whois-resources'}->{objects}->{object}->{attributes}->{attribute}
#			and $whois->{'whois-resources'}->{objects}->{object}->{attributes}->{attribute}){
#			foreach my $attr (@{ $whois->{'whois-resources'}->{objects}->{object}->{attributes}->{attribute} }){
#				if ($attr->{name} eq 'descr'){
#					$ret->{descr} = $ret->{descr} ? $ret->{descr} . ' ' . $attr->{value} : $attr->{value};
#					$ret->{name} = $ret->{descr};
#				}
#				elsif ($attr->{name} eq 'country'){
#					$ret->{cc} = $attr->{value};
#				}
#				elsif ($attr->{name} eq 'netname'){
#					$ret->{org} = $attr->{value};
#				}
#			}
#			$self->log->trace( 'set cache for ' . $ripe_url );
#			
#			$self->cache->set($ripe_url, {
#				cc => $ret->{cc},
#				descr => $ret->{descr},
#				name => $ret->{name},
#				org => $ret->{org},
#			});
#			$self->_update_records($ip, $ret);
#		}
		if ($whois->{objects}){
			foreach my $object (keys %{ $whois->{objects} }){
				next unless $whois->{objects}->{$object}->[0] 
					and $whois->{objects}->{$object}->[0]->{attributes}
					and $whois->{objects}->{$object}->[0]->{attributes}->{attribute};
				foreach my $attr (@{ $whois->{objects}->{$object}->[0]->{attributes}->{attribute} }){
					if ($attr->{name} eq 'descr'){
						$ret->{descr} = $ret->{descr} ? $ret->{descr} . ' ' . $attr->{value} : $attr->{value};
						$ret->{name} = $ret->{descr};
					}
					elsif ($attr->{name} eq 'country'){
						$ret->{cc} = $attr->{value};
					}
					elsif ($attr->{name} eq 'netname'){
						$ret->{org} = $attr->{value};
					}
				}
			}
			$self->log->trace( 'set cache for ' . $ripe_url );
			$self->cache->set($ripe_url, {
				cc => $ret->{cc},
				descr => $ret->{descr},
				name => $ret->{name},
				org => $ret->{org},
			});
			$self->_update_records($ip, $ret);
		}			
		else {
			$self->log->error( 'INVALID RIPE: ' . Dumper($whois) );
		}
		$self->cv->end;
	};
	return;
}

sub _lookup_org {
	my $self = shift;
	my $org_url = shift;
	my $ip = shift;
	
	$org_url =~ /\/([^\/]+)$/;
	my $key = $1;
	my $ret = {};
	
	if (my $cached = $self->cache->get($key, 
		expire_if => sub {
			my $obj = $_[0];
			if ($obj->value and $obj->value->{cc}){
				return 0;
			}
			else {
				$self->log->trace('expiring ' . $key);
				return 1;
			}
		}
		)){
		$self->log->trace('Using cached url ' . $org_url . ' with key ' . $key);
		$self->cache_stats->{hits}++;
		foreach my $key (keys %$cached){
			$ret->{$key} = $cached->{$key};
		}
		$self->_update_records($ip, $ret);
		$self->cv->end;
		return;
	}
	
	$self->log->trace( 'getting ' . $org_url );
	http_request GET => $org_url, timeout => $Timeout, %Proxy, headers => { Accept => 'application/json' }, sub {
		my ($body, $hdr) = @_;
		$self->log->trace('got body: ' . Dumper($body) . 'hdr: ' . Dumper($hdr));
		try {
			my $whois = decode_json($body);
			$self->log->trace('decoded whois: ' . Dumper($whois));
			foreach my $key (qw(org customer)){
				next unless $whois->{$key};
				if ($whois->{$key}->{'iso3166-1'}){
					$ret->{cc} = $whois->{$key}->{'iso3166-1'}->{code2}->{'$'};
					$ret->{country} = $whois->{$key}->{'iso3166-1'}->{name}->{'$'};
				}
				if ($whois->{org}->{'iso3166-2'}){
					$ret->{state} = $whois->{$key}->{'iso3166-2'}->{'$'};
				}
				if ($whois->{$key}->{city}){
					$ret->{city} = $whois->{$key}->{city}->{'$'};
				}
			}
			throw(500, 'Invalid data for ' . $org_url . ', only got ' . Dumper($ret), { external_http => $org_url }) unless $ret->{country};
			$self->log->trace( 'set cache for ' . $org_url . ' with key ' . $key);
			$self->cache_stats->{misses}++;
			my $data = { 
				cc => $ret->{cc},
				country => $ret->{country},
				state => $ret->{state},
				city => $ret->{city},
			};
			$self->log->trace('org cache data: ' . Dumper($data));
			$self->cache->set($key, $data);
			$self->_update_records($ip, $data);
		}
		catch {
			$self->log->error($_);
		};
		$self->cv->end;
		return;
	};
	$self->log->trace( 'sent ' . $org_url );
}

1;