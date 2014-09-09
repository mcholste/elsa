package Transform::CIF;
use Moose;
use Data::Dumper;
use CHI;
use DBI qw(:sql_types);
use Socket qw(inet_aton);
use AnyEvent::HTTP;
use URL::Encode qw(url_encode);
use JSON;
use Try::Tiny;
use Ouch qw(:trytiny);;
extends 'Transform';
with 'Utils';

our $Name = 'CIF';
our $Timeout = 10;
our $DefaultTimeOffset = 120;
our $Description = 'CIF lookup';
sub description { return $Description }
our $Fields = { map { $_ => 1 } qw(srcip dstip site hostname) };
# CIF plugin for https://github.com/mcholste/cif-rest-sphinx or native CIF
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'known_subnets' => (is => 'rw', isa => 'HashRef');
has 'known_orgs' => (is => 'rw', isa => 'HashRef');
has 'cv' => (is => 'rw', isa => 'Object');

sub BUILD {
	my $self = shift;
	
	if ($self->conf->get('transforms/whois/known_subnets')){
		$self->known_subnets($self->conf->get('transforms/whois/known_subnets'));
	}
	if ($self->conf->get('transforms/whois/known_orgs')){
		$self->known_orgs($self->conf->get('transforms/whois/known_orgs'));
	}
	
	my $keys = {};
	if (scalar @{ $self->args }){
		foreach my $arg (@{ $self->args }){
			$keys->{$arg} = 1;
		}
	}
	else {
		$keys = $Fields;
	}
		
	if ($self->conf->get('transforms/cif/server_ip') or $self->conf->get('transforms/cif/base_url')){
		$self->_query_native_cif($keys);
	}
	elsif($self->conf->get('transforms/cif/dsn')) {
		$self->_query_cif_rest_sphinx($keys);
	}
	else {
		throw('No CIF server_ip, base_url, or dsn configured');
	}
	
	return 1;
}

# Standard CIF web API query
sub _query_native_cif {
	my $self = shift;
	my $keys = shift;
	
	foreach my $record ($self->results->all_results){
		$record->{transforms}->{$Name} = {};
		
		$self->cv(AnyEvent->condvar);
		$self->cv->begin(sub { $self->on_transform->($self->results) });
		foreach my $key ($self->results->keys($record)){
			if ($keys->{$key}){
				$record->{transforms}->{$Name}->{$key} = {};
				$self->_query($record, $key, $self->results->value($record, $key));
			}
		}
		
		$self->cv->end;
	}
}

# Much faster query if using cif-rest-sphinx
sub _query_cif_rest_sphinx {
	my $self = shift;
	my $keys = shift;
	
	my $cif = DBI->connect($self->conf->get('transforms/cif/dsn'), '', '', 
		{ 
			RaiseError => 1,
			mysql_multi_statements => 1,
			mysql_bind_type_guessing => 1, 
		}) or throw(502, $DBI::errstr, { mysql => $self->conf->get('transforms/cif/dsn') });
	my ($query, $sth);
	$query = 'SELECT * FROM url, domain WHERE MATCH(?)';
	$sth = $cif->prepare($query);
	$query = 'SELECT * FROM infrastructure WHERE MATCH(?) AND subnet_start <= ? AND subnet_end >= ?';
	my $ip_sth = $cif->prepare($query);
	
	RECORD_LOOP: foreach my $record ($self->results->all_results){
		$record->{transforms}->{$Name} = {};
		foreach my $key ($self->results->keys($record)){
			my $value = $self->results->value($record, $key);
			if ($value and $Fields->{ $key }){
				$record->{transforms}->{$Name}->{$key} = {};
				my $info = $self->cache->get($value);
				if ($info and ref($info) eq 'HASH' and scalar keys %$info){
					$record->{transforms}->{$Name}->{$key} = $info;
					#$self->log->trace('using cached value for ' . $datum->{$key} . ': ' . Dumper($info));
					next;
				}
								
				my $row;
				# Handle IP's
				if ($value =~ /^(\d{1,3}\.\d{1,3}\.)\d{1,3}\.\d{1,3}$/){
					next if $self->_check_local($value);
					$self->log->trace('checking ' . $value);
					my $first_octets = $1;
					my $ip_int = unpack('N*', inet_aton($value));
					$ip_sth->bind_param(1, '@address ' . $first_octets . '* @description -search @alternativeid -www.alexa.com @alternativeid -support.clean-mx.de');
					$ip_sth->bind_param(2, $ip_int, SQL_INTEGER);
					$ip_sth->bind_param(3, $ip_int, SQL_INTEGER);
					$ip_sth->execute;
					$row = $ip_sth->fetchrow_hashref;
					if ($row){
						foreach my $col (keys %$row){
							next if $col eq 'weight';
							if ($col eq 'detecttime' or $col eq 'created'){
								$row->{$col} = epoch2iso($row->{$col});
							}
							$record->{transforms}->{$Name}->{$key}->{$col} = $row->{$col};
						}
						$self->cache->set($value, $record->{transforms}->{$Name}->{$key});
						next RECORD_LOOP;
					}
				}
				
				$self->log->trace('checking ' . $value);
				$sth->execute($value . ' -@description search');
				$row = $sth->fetchrow_hashref;
			
				unless ($row){
					$self->cache->set($value, {});
					next;
				}
				
				foreach my $col (keys %$row){
					next if $col eq 'weight';
					if ($col eq 'detecttime' or $col eq 'created'){
						$row->{$col} = epoch2iso($row->{$col});
					}
					$record->{transforms}->{$Name}->{$key}->{$col} = $row->{$col};
				}
				$self->cache->set($record->{$key}, $record->{transforms}->{$Name}->{$key});
				next RECORD_LOOP;
			}
		}
	}
}	

sub _check_local {
	my $self = shift;
	my $ip = shift;
	my $ip_int = unpack('N*', inet_aton($ip));
	
	return unless $ip_int and $self->known_subnets and $self->known_orgs;
	
	foreach my $start (keys %{ $self->known_subnets }){
		if (unpack('N*', inet_aton($start)) <= $ip_int 
			and unpack('N*', inet_aton($self->known_subnets->{$start}->{end})) >= $ip_int){
			return 1;
		}
	}
}

sub _query {
	my $self = shift;
	my $record = shift;
	my $key = shift;
	my $query = shift;
	
	if ($self->_check_local($query)){
		$record->{transforms}->{$Name}->{$key} = {};
		return;
	}
	
	$self->cv->begin;
	
	$query = url_encode($query);
	my $url;
	if ($self->conf->get('transforms/cif/server_ip')){
		$url = sprintf('http://%s/api/%s?apikey=%s&fmt=json&query=%s', 
			$self->conf->get('transforms/cif/server_ip'), $query, 
			$self->conf->get('transforms/cif/apikey'), $query);
	}
	elsif ($self->conf->get('transforms/cif/base_url')){
		$url = sprintf('%s/api/%s?apikey=%s&fmt=json&query=%s', 
			$self->conf->get('transforms/cif/base_url'), $query, 
			$self->conf->get('transforms/cif/apikey'), $query);
	}
	else {
		throw(500, 'server_ip nor base_url configured', { config => 'server_ip or base_url' });
	}
	
	my $info = $self->cache->get($url, expire_if => sub {
		my $obj = $_[0];
		eval {
			my $data = $obj->value;
			#$self->log->debug('data: ' . Dumper($data));
			unless (scalar keys %{ $data }){
				$self->log->debug('expiring ' . $url);
				return 1;
			}
		};
		if ($@){
			$self->log->debug('error: ' . $@ . 'value: ' . Dumper($obj->value) . ', expiring ' . $url);
			return 1;
		}
		return 0;
	});
	if ($info){
		$record->{transforms}->{$Name}->{$key} = $info;
		$self->cv->end;
		return;
	}
	
	$self->log->debug('getting ' . $url);
	my $headers = {
		Accept => 'application/json',
	};
	if ($self->conf->get('transforms/cif/server_name')){
		$headers->{Host} = $self->conf->get('transforms/cif/server_name');
	}
	http_request GET => $url, headers => $headers, sub {
		my ($body, $hdr) = @_;
		my $data;
		eval {
			$data = decode_json($body);
		};
		if ($@){
			#$self->log->error($@ . 'hdr: ' . Dumper($hdr) . ', url: ' . $url . ', body: ' . ($body ? $body : ''));
			# This may be a multiline pseudo-json format indicating v1.0
			eval {
				$data = [ map { decode_json($_) } split("\n", $body) ];
				$self->log->debug('added ' . (scalar @$data) . ' v1.0 entries');
			};
			if ($@){
				$self->log->error($@ . 'hdr: ' . Dumper($hdr) . ', url: ' . $url . ', body: ' . ($body ? $body : ''));
				$self->cv->end;
				return;
			}
		}
				
		if ($data and ref($data) eq 'HASH' and $data->{status} eq '200' and $data->{data}->{feed} and $data->{data}->{feed}->{entry}){
			foreach my $entry ( @{ $data->{data}->{feed}->{entry}} ){
				my $cif_datum = {};
				if ($entry->{Incident}){
					if ($entry->{Incident}->{Assessment}){
						if ($entry->{Incident}->{Assessment}->{Impact}){
							$self->log->debug('$entry' . Dumper($entry));
							if (ref($entry->{Incident}->{Assessment}->{Impact})){
								$cif_datum->{type} = $entry->{Incident}->{Assessment}->{Impact}->{content};
								$cif_datum->{severity} = $entry->{Incident}->{Assessment}->{Impact}->{severity};
							}
							else {
								$cif_datum->{type} = $entry->{Incident}->{Assessment}->{Impact};
								$cif_datum->{severity} = 'low';
							}
						}
						if ($entry->{Incident}->{Assessment}->{Confidence}){
							$cif_datum->{confidence} = $entry->{Incident}->{Assessment}->{Confidence}->{content};
						}
					}
					
					$cif_datum->{timestamp} = $entry->{Incident}->{DetectTime};
					
					if ($entry->{Incident}->{EventData}){
						if ($entry->{Incident}->{EventData}->{Flow}){
							if ($entry->{Incident}->{EventData}->{Flow}->{System}){
								if ($entry->{Incident}->{EventData}->{Flow}->{System}->{Node}){
									if ($entry->{Incident}->{EventData}->{Flow}->{System}->{Node}->{Address}){
										my $add = $entry->{Incident}->{EventData}->{Flow}->{System}->{Node}->{Address};
										if (ref($add) eq 'HASH'){
											$cif_datum->{ $add->{'ext-category'} } = $add->{content};
										}
										else {
											$cif_datum->{ip} = $add;
										}
									}
								}
								if ($entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData}){
									if (ref($entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData}) eq 'ARRAY'){
										$cif_datum->{description} = '';
										foreach my $add (@{ $entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData} }){
											$cif_datum->{description} .= $add->{meaning} . '=' . $add->{content} . ' ';
										}
									}
									elsif (ref($entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData}) eq 'HASH'){
										my $add = $entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData};
										$cif_datum->{description} = $add->{meaning} . '=' . $add->{content};
									}
									else {
										$cif_datum->{description} = $entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData};
									}
								}
							}
						}
					}
					
					if ($entry->{Incident}->{AlternativeID}){
						if ($entry->{Incident}->{AlternativeID}->{IncidentID}){
							if ($entry->{Incident}->{AlternativeID}->{IncidentID}->{content}){
								$cif_datum->{reference} = $entry->{Incident}->{AlternativeID}->{IncidentID}->{content};
							}
						}
					}
					
					if ($entry->{Incident}->{Description}){
						$cif_datum->{reason} = $entry->{Incident}->{Description};
					}
					foreach my $cif_key (keys %$cif_datum){
						$record->{transforms}->{$Name}->{$key}->{$cif_key} ||= {};
						$record->{transforms}->{$Name}->{$key}->{$cif_key}->{ $cif_datum->{$cif_key} } = 1;
					}
					#$datum->{transforms}->{$Name}->{$key} = $cif_datum;
					#$self->cache->set($url, $cif_datum);
				}
			}
			my $final = {};
			foreach my $cif_key (sort keys %{ $record->{transforms}->{$Name}->{$key} }){
				$final->{$cif_key} = join(' ', sort keys %{ $record->{transforms}->{$Name}->{$key}->{$cif_key} });
			}
			$record->{transforms}->{$Name}->{$key} = $final;
					
			$self->cache->set($url, $record->{transforms}->{$Name}->{$key});
		}
		# CIF v1.0 format
		elsif ($data and ref($data) eq 'ARRAY'){
			foreach my $entry ( @$data ){
				my $cif_datum = {};
				$cif_datum->{type} = $entry->{assessment};
				$cif_datum->{severity} = $entry->{severity};
				$cif_datum->{timestamp} = $entry->{detecttime};
				$cif_datum->{address} = $entry->{address};
				$cif_datum->{description} = $entry->{description};
				$cif_datum->{confidence} = $entry->{confidence};
				$cif_datum->{reference} = $entry->{alternativeid};
				$cif_datum->{reason} = $entry->{purpose};
				$self->log->debug('cif_datum: ' . Dumper($cif_datum));
					
				foreach my $cif_key (keys %$cif_datum){
					$record->{transforms}->{$Name}->{$key}->{$cif_key} ||= {};
					$record->{transforms}->{$Name}->{$key}->{$cif_key}->{ $cif_datum->{$cif_key} } = 1;
				}
			}
			my $final = {};
			foreach my $cif_key (sort keys %{ $record->{transforms}->{$Name}->{$key} }){
				$final->{$cif_key} = join(' ', sort keys %{ $record->{transforms}->{$Name}->{$key}->{$cif_key} });
			}
			$record->{transforms}->{$Name}->{$key} = $final;
					
			$self->cache->set($url, $record->{transforms}->{$Name}->{$key});
		}
		$self->cv->end;
	};
}
 
1;