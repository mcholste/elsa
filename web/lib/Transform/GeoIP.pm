package Transform::GeoIP;
use Moose;
use Data::Dumper;
use Socket;
use JSON;
use Geo::IP; # get additional databases from Maxmind.com
extends 'Transform';
our $Name = 'GeoIP';
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILD {
	my $self = shift;
	
	my $geoip;
	my $cc_only = 0;
	eval {
		$geoip = Geo::IP->open_type(GEOIP_CITY_EDITION_REV1, GEOIP_MEMORY_CACHE) or die('Unable to create GeoIP object: ' . $!);
	};
	if ($@){
		$self->log->warn('GeoIP city edition not found, falling back to country edition');
		$geoip = Geo::IP->open_type(GEOIP_COUNTRY_EDITION, GEOIP_MEMORY_CACHE) or die('Unable to create GeoIP object: ' . $!);
		$cc_only = 1;
	}
	
	foreach my $record ($self->results->all_results){
		$record->{transforms}->{$Name} = {};
		foreach my $key ($self->results->keys($record)){
			my $value = $self->results->value($record, $key);
			if ($key eq 'hostname'){
				$record->{transforms}->{$Name}->{$key} = $geoip->record_by_name($value);
			}
			elsif ($value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
				if ($cc_only){
					my $cc = $geoip->country_code_by_addr($value);
					next unless $cc;
					$record->{transforms}->{$Name}->{$key} = {
						cc => $cc
					}
				}
				else {
					my $geo_rec = $geoip->record_by_addr($value);
					next unless $geo_rec;
					$record->{transforms}->{$Name}->{$key} = {
						cc => $geo_rec->country_code,
						latitude => $geo_rec->latitude,
						longitude => $geo_rec->longitude,
						state => $geo_rec->region,
						city => $geo_rec->city,
						country => $geo_rec->country_name,
					};
					foreach my $rec_key (keys %{ $record->{transforms}->{$Name}->{$key} }){
						delete $record->{transforms}->{$Name}->{$key}->{$rec_key} unless defined $record->{transforms}->{$Name}->{$key}->{$rec_key};
					}
				}
			}
		}
	}
	
	$self->on_transform->($self->results);
	
	return $self;
}
 
1;