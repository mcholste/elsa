package Connector::StreamGrep;
use Moose;
use Data::Dumper;
use AnyEvent::HTTP;
use Date::Manip;
extends 'Connector';

our $Timeout = 10;
our $DefaultTimeOffset = 120;
our $Description = 'Grep streams for each record';
sub description { return $Description }
sub admin_required { return 1 }

has 'regex' => (is => 'ro', required => 1, default => sub { qr/./ });

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if ($params{args}->[0] and ref($params{args}->[0]) and $params{args}->[0]->[0]){
		$params{regex} = $params{args}->[0]->[0];
	}
	elsif ($params{args}->[0]){
		$params{regex} = $params{args}->[0];
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	my $cv = AnyEvent->condvar;
	my @results;
	foreach my $record (@{ $self->results->{results} }){
		my %args;
		foreach my $field_hash (@{ $record->{_fields} }){
			if ($field_hash->{field} =~ /srcip/){
				$args{srcip} = $field_hash->{value};
			}
			elsif ($field_hash->{field} =~ /dstip/){
				$args{dstip} = $field_hash->{value};
			}
			elsif ($field_hash->{field} =~ /srcport/){
				$args{srcport} = $field_hash->{value};
			}
			elsif ($field_hash->{field} =~ /dstport/){
				$args{dstport} = $field_hash->{value};
			}
		}
		next unless $args{srcip} and $args{dstip};
		my $timestamp = $record->{timestamp};
		my $start = $timestamp - $DefaultTimeOffset;
		my $end = $timestamp + $DefaultTimeOffset;
		my $req_str = $self->api->conf->get('connectors/sandbox/url') . '/?as_json=1&start=' . $start . '&end=' . $end;
		foreach my $arg (keys %args){
			$req_str .= '&' . $arg . '=' . $args{$arg};
		}
		$self->api->log->debug('regex: ' . Dumper($self->regex));
		$req_str .= '&pcre=' . $self->regex;
		$self->api->log->trace('sending to url ' . $req_str);
		my %headers = (timeout => $Timeout);
		# Apache won't use the /etc/hosts file for resolving names, so you need to set the site here as the name of
		#  the sandbox Apache vhost and use the IP in the URL to make the request. This is only necessary if 
		#  your site won't resolve using DNS.
		if ($self->api->conf->get('connectors/sandbox/site')){
			$headers{Host} = $self->api->conf->get('connectors/sandbox/site');
		}
		
		$cv->begin;
		http_get($req_str,
			headers => \%headers,
			sub {
				my ($body, $hdr) = @_;
				if ($!){
					$self->api->log->error('error: ' . $!);
				}
				$self->api->log->trace('body: ' . Dumper($body));
				my $subresults = $self->api->json->decode($body);
				foreach my $subresult (@$subresults){ 
					push @results, $subresult;
				}
				$self->api->log->trace('results: ' . Dumper(\@results));
				$cv->end;
			}
		);
	}
	$cv->recv;
	
	$self->results({ results => \@results });
	
	return 1;
}


1