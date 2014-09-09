package Connector::CIF;
use Moose;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Request::Common;
use Socket;
extends 'Connector';

our $Timeout = 10;
our $DefaultTimeOffset = 120;
our $Description = 'Insert into CIF';
sub description { return $Description }
sub admin_required { return 1 }
our $Fields = { map { $_ => 1 } qw(srcip dstip site hostname) };

has 'known_subnets' => (is => 'rw', isa => 'HashRef');
has 'known_orgs' => (is => 'rw', isa => 'HashRef');
has 'field' => (is => 'rw', isa => 'Str');
has 'restriction' => (is => 'rw', isa => 'Str', required => 1, default => 'need-to-know');
has 'cif_description' => (is => 'rw', isa => 'Str', required => 1, default => 'infrastructure');
has 'confidence' => (is => 'rw', isa => 'Num');

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if ($params{args} and ref($params{args}) eq 'ARRAY'){
		my ($description, $arg1, $arg2) = @{ $params{args} };
		# Try to figure out which arg is confidence and which is field
		if ($arg1 and $arg1 =~ /^\d+$/){
			$params{confidence} = $arg1;
			if ($arg2){
				$params{field} = $arg2;
			}
		}
		elsif ($arg1){
			$params{field} = $arg1;
			if ($arg2){
				$params{confidence} = $arg2;
			}
		}
			
		if ($params{field}){
			unless ($Fields->{$params{field}}){
				die('Invalid field');
			}
		}
		if ($description){
			$params{cif_description} = $description;
		}
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	if ($self->api->conf->get('transforms/whois/known_subnets')){
		$self->known_subnets($self->api->conf->get('transforms/whois/known_subnets'));
	}
	if ($self->api->conf->get('transforms/whois/known_orgs')){
		$self->known_orgs($self->api->conf->get('transforms/whois/known_orgs'));
	}
	
	# given, from config, or default to 95
	my $confidence = $self->confidence ? $self->confidence : $self->api->conf->get('connectors/cif/confidence') ? $self->api->conf->get('connectors/cif/confidence') : 95;
	
	my $info = { 
		source => $self->api->conf->get('connectors/cif/source_name') ? $self->api->conf->get('connectors/cif/source_name') : 'ELSA',
		description => $self->cif_description,
		restriction => $self->restriction,
		impact => $self->cif_description,
		confidence => $confidence,
		severity => $self->api->conf->get('connectors/cif/severity') ? $self->api->conf->get('connectors/cif/severity') : 'high',
	};
	
	my @to_insert;
	my $invalid = 0;
	RECORD_LOOP: foreach my $record (@{ $self->results->{results} }){
		my $value = $self->_get_value($record);
		if ($value){
			$info->{address} = $value;
			push @to_insert, { %$info };
		}
		else {
			$invalid++;
		}
	}
	$self->api->add_warning(500, 'Found ' . $invalid . ' values', { connector => 'CIF' }) if $invalid;
	return 1 unless scalar @to_insert;
	
	my $send = $self->api->json->encode([@to_insert]);
	$self->api->log->debug('send: ' . $send);
	my $ua = new LWP::UserAgent();
	my $req = POST($self->api->conf->get('connectors/cif/url'),	Content => $send);
	my $res = $ua->request($req);
	my $ret = $res->content();
	$self->api->log->debug('got ret: ' . Dumper($ret));
	$ret = $self->api->json->decode($ret);
	my @uuids;
	foreach my $uuid (@{ $ret->{data} }){
		push @uuids, $uuid;
	}
	$self->results( { results => [ 'Successfully added new uuid\'s: ' . join(', ', @uuids) ] } );
		
	return 1;
}

sub _get_value {
	my $self = shift;
	my $record = shift;
	
	# pick field
	unless ($self->field){
		# Check to see if we have class-specific config for guidance
		my $field_selection = $self->api->conf->get('connectors/cif/field_selection');
		if ($field_selection and $field_selection->{ $record->{class} }){
			$self->field($field_selection->{ $record->{class} });
		}
		else {
			$self->field('any');
		}
	}
	
	foreach my $field_hash (@{ $record->{_fields} }){
		if ($self->field eq 'any'){
			if ($Fields->{ $field_hash->{field} }){
				my $value = $self->_check_value($field_hash->{value});
				return $value if $value;
			}
		}
		elsif ($field_hash->{field} eq $self->field){
			return $self->_check_value($field_hash->{value});
		}
	}
}

sub _check_value {
	my $self = shift;
	my $value = shift;
	
	# Handle IP's
	if ($value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
		return if $self->_check_local($value);
	}
	# Handle DNS
	unless ($value =~ /^[\w\-\.]+$/){
		$self->log->error('Invalid hostname value ' . $value);
		return;
	}
	return $value;
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


1