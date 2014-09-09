package Fields;
use Moose::Role;
with 'MooseX::Traits';
use Data::Dumper;
use Sys::Hostname::FQDN;
use Net::DNS;
use String::CRC32;
use Socket qw(inet_aton inet_ntoa);
use Try::Tiny;
use Ouch qw(:trytiny);
use Exporter qw(import);

our @EXPORT = qw(epoch2iso);

our $Field_order_to_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	4 => 'msg',
	5 => 'attr_i0',
	6 => 'attr_i1',
	7 => 'attr_i2',
	8 => 'attr_i3',
	9 => 'attr_i4',
	10 => 'attr_i5',
	11 => 'attr_s0',
	12 => 'attr_s1',
	13 => 'attr_s2',
	14 => 'attr_s3',
	15 => 'attr_s4',
	16 => 'attr_s5',
};

our $Field_order_to_meta_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	4 => 'msg',
};

our $Field_order_to_field = {
	1 => 'host',
	2 => 'program',
	3 => 'class',
	4 => 'msg',
	5 => 'i0',
	6 => 'i1',
	7 => 'i2',
	8 => 'i3',
	9 => 'i4',
	10 => 'i5',
	11 => 's0',
	12 => 's1',
	13 => 's2',
	14 => 's3',
	15 => 's4',
	16 => 's5',
};

our $Field_to_order = {
	'timestamp' => 0,
	'minute' => 100,
	'hour' => 101,
	'day' => 102,
	'host' => 1,
	'program' => 2,
	'class' => 3,
	'msg' => 4,
	'i0' => 5,
	'i1' => 6,
	'i2' => 7,
	'i3' => 8,
	'i4' => 9,
	'i5' => 10,
	's0' => 11,
	's1' => 12,
	's2' => 13,
	's3' => 14,
	's4' => 15,
	's5' => 16,
};

our $Proto_map = {
	'HOPOPT' => 0,
	'ICMP' => 1,
	'IGMP' => 2,
	'GGP' => 3,
	'IPV4' => 4,
	'ST' => 5,
	'TCP' => 6,
	'CBT' => 7,
	'EGP' => 8,
	'IGP' => 9,
	'BBN-RCC-MON' => 10,
	'NVP-II' => 11,
	'PUP' => 12,
	'ARGUS' => 13,
	'EMCON' => 14,
	'XNET' => 15,
	'CHAOS' => 16,
	'UDP' => 17,
	'MUX' => 18,
	'DCN-MEAS' => 19,
	'HMP' => 20,
	'PRM' => 21,
	'XNS-IDP' => 22,
	'TRUNK-1' => 23,
	'TRUNK-2' => 24,
	'LEAF-1' => 25,
	'LEAF-2' => 26,
	'RDP' => 27,
	'IRTP' => 28,
	'ISO-TP4' => 29,
	'NETBLT' => 30,
	'MFE-NSP' => 31,
	'MERIT-INP' => 32,
	'DCCP' => 33,
	'3PC' => 34,
	'IDPR' => 35,
	'XTP' => 36,
	'DDP' => 37,
	'IDPR-CMTP' => 38,
	'TP++' => 39,
	'IL' => 40,
	'IPV6' => 41,
	'SDRP' => 42,
	'IPV6-ROUTE' => 43,
	'IPV6-FRAG' => 44,
	'IDRP' => 45,
	'RSVP' => 46,
	'GRE' => 47,
	'DSR' => 48,
	'BNA' => 49,
	'ESP' => 50,
	'AH' => 51,
	'I-NLSP' => 52,
	'SWIPE' => 53,
	'NARP' => 54,
	'MOBILE' => 55,
	'TLSP' => 56,
	'SKIP' => 57,
	'IPV6-ICMP' => 58,
	'IPV6-NONXT' => 59,
	'IPV6-OPTS' => 60,
	'CFTP' => 62,
	'SAT-EXPAK' => 64,
	'KRYPTOLAN' => 65,
	'RVD' => 66,
	'IPPC' => 67,
	'SAT-MON' => 69,
	'VISA' => 70,
	'IPCV' => 71,
	'CPNX' => 72,
	'CPHB' => 73,
	'WSN' => 74,
	'PVP' => 75,
	'BR-SAT-MON' => 76,
	'SUN-ND' => 77,
	'WB-MON' => 78,
	'WB-EXPAK' => 79,
	'ISO-IP' => 80,
	'VMTP' => 81,
	'SECURE-VMTP' => 82,
	'VINES' => 83,
	'IPTM' => 84,
	'TTP' => 84,
	'NSFNET-IGP' => 85,
	'DGP' => 86,
	'TCF' => 87,
	'EIGRP' => 88,
	'OSPFIGP' => 89,
	'SPRITE-RPC' => 90,
	'LARP' => 91,
	'MTP' => 92,
	'AX.25' => 93,
	'IPIP' => 94,
	'MICP' => 95,
	'SCC-SP' => 96,
	'ETHERIP' => 97,
	'ENCAP' => 98,
	'GMTP' => 100,
	'IFMP' => 101,
	'PNNI' => 102,
	'PIM' => 103,
	'ARIS' => 104,
	'SCPS' => 105,
	'QNX' => 106,
	'A/N' => 107,
	'IPCOMP' => 108,
	'SNP' => 109,
	'COMPAQ-PEER' => 110,
	'IPX-IN-IP' => 111,
	'VRRP' => 112,
	'PGM' => 113,
	'L2TP' => 115,
	'DDX' => 116,
	'IATP' => 117,
	'STP' => 118,
	'SRP' => 119,
	'UTI' => 120,
	'SMP' => 121,
	'SM' => 122,
	'PTP' => 123,
	'ISIS OVER IPV4' => 124,
	'FIRE' => 125,
	'CRTP' => 126,
	'CRUDP' => 127,
	'SSCOPMCE' => 128,
	'IPLT' => 129,
	'SPS' => 130,
	'PIPE' => 131,
	'SCTP' => 132,
	'FC' => 133,
	'RSVP-E2E-IGNORE' => 134,
	'MOBILITY HEADER' => 135,
	'UDPLITE' => 136,
	'MPLS-IN-IP' => 137,
	'MANET' => 138,
	'HIP' => 139,
	'SHIM6' => 140,
	'WESP' => 141,
	'ROHC' => 142,
	'RESERVED' => 255,
};

our $Inverse_proto_map = {
	0 => 'HOPOPT',
	1 => 'ICMP',
	2 => 'IGMP',
	3 => 'GGP',
	4 => 'IPV4',
	5 => 'ST',
	6 => 'TCP',
	7 => 'CBT',
	8 => 'EGP',
	9 => 'IGP',
	10 => 'BBN-RCC-MON',
	11 => 'NVP-II',
	12 => 'PUP',
	13 => 'ARGUS',
	14 => 'EMCON',
	15 => 'XNET',
	16 => 'CHAOS',
	17 => 'UDP',
	18 => 'MUX',
	19 => 'DCN-MEAS',
	20 => 'HMP',
	21 => 'PRM',
	22 => 'XNS-IDP',
	23 => 'TRUNK-1',
	24 => 'TRUNK-2',
	25 => 'LEAF-1',
	26 => 'LEAF-2',
	27 => 'RDP',
	28 => 'IRTP',
	29 => 'ISO-TP4',
	30 => 'NETBLT',
	31 => 'MFE-NSP',
	32 => 'MERIT-INP',
	33 => 'DCCP',
	34 => '3PC',
	35 => 'IDPR',
	36 => 'XTP',
	37 => 'DDP',
	38 => 'IDPR-CMTP',
	39 => 'TP++',
	40 => 'IL',
	41 => 'IPV6',
	42 => 'SDRP',
	43 => 'IPV6-ROUTE',
	44 => 'IPV6-FRAG',
	45 => 'IDRP',
	46 => 'RSVP',
	47 => 'GRE',
	48 => 'DSR',
	49 => 'BNA',
	50 => 'ESP',
	51 => 'AH',
	52 => 'I-NLSP',
	53 => 'SWIPE',
	54 => 'NARP',
	55 => 'MOBILE',
	56 => 'TLSP',
	57 => 'SKIP',
	58 => 'IPV6-ICMP',
	59 => 'IPV6-NONXT',
	60 => 'IPV6-OPTS',
	62 => 'CFTP',
	64 => 'SAT-EXPAK',
	65 => 'KRYPTOLAN',
	66 => 'RVD',
	67 => 'IPPC',
	69 => 'SAT-MON',
	70 => 'VISA',
	71 => 'IPCV',
	72 => 'CPNX',
	73 => 'CPHB',
	74 => 'WSN',
	75 => 'PVP',
	76 => 'BR-SAT-MON',
	77 => 'SUN-ND',
	78 => 'WB-MON',
	79 => 'WB-EXPAK',
	80 => 'ISO-IP',
	81 => 'VMTP',
	82 => 'SECURE-VMTP',
	83 => 'VINES',
	84 => 'IPTM',
	85 => 'NSFNET-IGP',
	86 => 'DGP',
	87 => 'TCF',
	88 => 'EIGRP',
	89 => 'OSPFIGP',
	90 => 'SPRITE-RPC',
	91 => 'LARP',
	92 => 'MTP',
	93 => 'AX.25',
	94 => 'IPIP',
	95 => 'MICP',
	96 => 'SCC-SP',
	97 => 'ETHERIP',
	98 => 'ENCAP',
	100 => 'GMTP',
	101 => 'IFMP',
	102 => 'PNNI',
	103 => 'PIM',
	104 => 'ARIS',
	105 => 'SCPS',
	106 => 'QNX',
	107 => 'A/N',
	108 => 'IPCOMP',
	109 => 'SNP',
	110 => 'COMPAQ-PEER',
	111 => 'IPX-IN-IP',
	112 => 'VRRP',
	113 => 'PGM',
	115 => 'L2TP',
	116 => 'DDX',
	117 => 'IATP',
	118 => 'STP',
	119 => 'SRP',
	120 => 'UTI',
	121 => 'SMP',
	122 => 'SM',
	123 => 'PTP',
	124 => 'ISIS OVER IPV4',
	125 => 'FIRE',
	126 => 'CRTP',
	127 => 'CRUDP',
	128 => 'SSCOPMCE',
	129 => 'IPLT',
	130 => 'SPS',
	131 => 'PIPE',
	132 => 'SCTP',
	133 => 'FC',
	134 => 'RSVP-E2E-IGNORE',
	135 => 'MOBILITY HEADER',
	136 => 'UDPLITE',
	137 => 'MPLS-IN-IP',
	138 => 'MANET',
	139 => 'HIP',
	140 => 'SHIM6',
	141 => 'WESP',
	142 => 'ROHC',
	255 => 'RESERVED',
};

our $Time_values = {
	timestamp => 1,
	minute => 60,
	hour => 3600,
	day => 86400,
	week => 86400 * 7,
	month => 86400 * 30,
	year => 86400 * 365,
};

our $Reserved_fields = { map { $_ => 1 } qw( start end limit offset groupby node cutoff datasource timeout archive analytics nobatch livetail orderby orderby_dir ), keys %$Time_values };

our $IP_fields = { map { $_ => 1 } qw( node_id host_id ip srcip dstip sourceip destip ) };

our $Import_min_id = unpack('N*', inet_aton('127.0.0.2'));
our $Import_max_id = unpack('N*', inet_aton('127.255.255.255'));
our $Import_fields = [ qw(import_name import_description import_type import_date import_id) ];

# Helper methods for dealing with resolving fields
has 'node_info' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

sub epoch2iso {
	my $epochdate = shift;
	my $use_gm_time = shift;
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	if ($use_gm_time){
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($epochdate);
	}
	else {
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	}
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub resolve {
	my $self = shift;
	my $raw_field = shift;
	my $raw_value = shift;
	my $operator = shift;
	
	# Return all possible class_id, real_field, real_value combos
	$self->log->debug("resolving: raw_field: $raw_field, raw_value: $raw_value, operator: $operator");
	
	my %values = ( fields => {}, attrs => {} );
	# Find all possible real fields/classes for this raw field
	
	my $operator_xlate = {
		'=' => 'and',
		'' => 'or',
		'-' => 'not',
	};

	my $field_infos = $self->get_field($raw_field);
	$self->log->trace('field_infos: ' . Dumper($field_infos));
	foreach my $class_id (keys %{$field_infos}){
		if (scalar keys %{ $self->classes->{given} } and not $self->classes->{given}->{0}){
			unless ($self->classes->{given}->{$class_id} or $class_id == 0){
				$self->log->debug("Skipping class $class_id because it was not given");
				next;
			}
		}
		# we don't want to count class_id 0 as "distinct"
		if ($class_id){
			$self->classes->{distinct}->{$class_id} = 1;
		}
		
		my $field_order = $field_infos->{$class_id}->{field_order};
		# Check for string match and make that a term
		if ($field_infos->{$class_id}->{field_type} eq 'string' and
			($operator eq '=' or $operator eq '-' or $operator eq '')){
			$values{fields}->{$class_id}->{ $Field_order_to_field->{ $field_order } } = $raw_value;
			$values{attrs}->{$class_id}->{ $Field_order_to_attr->{ $field_order } } = crc32($raw_value);
		}
		elsif ($field_infos->{$class_id}->{field_type} eq 'string'){
			throw(400, 'Invalid operator for string field', { operator => $operator });
		}
		# Edge case for int fields stuffed into string attributes due to lack of space in the int attributes
		elsif ($field_infos->{$class_id}->{field_type} eq 'int' and $Field_order_to_attr->{ $field_order } =~ /^attr_s/){
			$values{fields}->{$class_id}->{ $Field_order_to_field->{ $field_order } } = $raw_value;
			$values{attrs}->{$class_id}->{ $Field_order_to_attr->{ $field_order } } = crc32($raw_value);
		}
		elsif ($Field_order_to_attr->{ $field_order }){
			$values{attrs}->{$class_id}->{ $Field_order_to_attr->{ $field_order } } =
				$self->normalize_value($class_id, $raw_value, $field_order);			
		}
		else {
			$self->log->warn("Unknown field: $raw_field");
		}
	}
	$self->log->trace('values: ' . Dumper(\%values));
	return \%values;
}

sub normalize_value {
	my $self = shift;
	my $class_id = shift;
	my $value = shift;
	my $field_order = shift;
	
	my $orig_value = $value;
	$value =~ s/^\"//;
	$value =~ s/\"$//;
	
	#$self->log->trace('args: ' . Dumper($args) . ' value: ' . $value . ' field_order: ' . $field_order);
	
	unless (defined $class_id and defined $value and defined $field_order){
		$self->log->error('Missing an arg: ' . $class_id . ', ' . $value . ', ' . $field_order);
		return $value;
	}
	
	return $value unless $self->meta_info->{field_conversions}->{ $class_id };
	
	if ($field_order == $Field_to_order->{host}){ #host is handled specially
		my @ret;
		if ($value =~ /^"?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"?$/) {
			@ret = ( unpack('N*', inet_aton($1)) ); 
		}
		elsif ($value =~ /^"?([a-zA-Z0-9\-\.]+)"?$/){
			my $host_to_resolve = $1;
			unless ($host_to_resolve =~ /\./){
				my $fqdn_hostname = Sys::Hostname::FQDN::fqdn();
				$fqdn_hostname =~ /^[^\.]+\.(.+)/;
				my $domain = $1;
				$self->log->debug('non-fqdn given, assuming to be domain: ' . $domain);
				$host_to_resolve .= '.' . $domain;
			}
			$self->log->debug('resolving and converting host ' . $host_to_resolve. ' to inet_aton');
			my $res   = Net::DNS::Resolver->new;
			my $query = $res->search($host_to_resolve);
			if ($query){
				my @ips;
				foreach my $rr ($query->answer){
					next unless $rr->type eq "A";
					$self->log->debug('resolved host ' . $host_to_resolve . ' to ' . $rr->address);
					push @ips, $rr->address;
				}
				if (scalar @ips){
					foreach my $ip (@ips){
						my $ip_int = unpack('N*', inet_aton($ip));
						push @ret, $ip_int;
					}
				}
				else {
					throw(500, 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring, { external_dns => $host_to_resolve });
				}
			}
			else {
				throw(500, 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring, { external_dns => $host_to_resolve });
			}
		}
		else {
			throw(400, 'Invalid host given: ' . Dumper($value), { host => $value });
		}
		if (wantarray){
			return @ret;
		}
		else {
			return $ret[0];
		}
	}
	elsif ($field_order == $Field_to_order->{class}){
		return $self->meta_info->{classes}->{ uc($value) };
	}
	elsif ($self->meta_info->{field_conversions}->{ $class_id }->{'IPv4'}
		and $self->meta_info->{field_conversions}->{ $class_id }->{'IPv4'}->{$field_order}
		and $value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
		return unpack('N', inet_aton($value));
	}
	elsif ($self->meta_info->{field_conversions}->{ $class_id }->{PROTO} 
		and $self->meta_info->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		$self->log->trace("Converting $value to proto");
		return exists $Proto_map->{ uc($value) } ? $Proto_map->{ uc($value) } : int($value);
	}
	elsif ($self->meta_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $self->meta_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		if ($Field_order_to_attr->{$field_order} =~ /attr_s/){
			$self->log->trace("Converting $value to CRC of country_code");
			return crc32(join('', unpack('c*', pack('A*', uc($value)))));
		}
		else {
			$self->log->trace("Converting $value to country_code");
			return join('', unpack('c*', pack('A*', uc($value))));
		}
	}
	elsif ($Field_order_to_attr->{$field_order} eq 'program_id'){
		$self->log->trace("Converting $value to attr");
		return crc32($value);
	}
	elsif ($Field_order_to_attr->{$field_order} =~ /^attr_s\d+$/){
		# String attributes need to be crc'd
		return crc32($value);
	}
	else {
		# Integer value
		if ($orig_value == 0 or int($orig_value)){
			return $orig_value;
		}
		else {
			# Try to find an int and use that
			$orig_value =~ s/\\?\s//g;
			if (int($orig_value)){
				return $orig_value;
			}
			else {
				throw(400, 'Invalid query term, not an integer: ' . $orig_value, { term => $orig_value });
			}
		}
	}
}


sub get_field {
	my $self = shift;
	my $raw_field = shift;
			
	# Account for FQDN fields which come with the class name
	my ($class, $field) = split(/\./, $raw_field);
	
	if ($field){
		# We were given an FQDN, so there is only one class this can be
		foreach my $field_hash (@{ $self->node_info->{fields} }){
			if (lc($field_hash->{fqdn_field}) eq lc($raw_field)){
				return { $self->meta_info->{classes}->{uc($class)} => $field_hash };
			}
		}
	}
	else {
		# Was not FQDN
		$field = $raw_field;
	}
	
	$class = 0;
	my %fields;
		
	# Could also be a meta-field/attribute
	if (defined $Field_to_order->{$field}){
		$fields{$class} = { 
			value => $field, 
			text => uc($field), 
			field_id => $Field_to_order->{$field},
			class_id => $class, 
			field_order => $Field_to_order->{$field},
			field_type => 'int',
		};
	}
		
	foreach my $row (@{ $self->meta_info->{fields} }){
		if ($row->{value} eq $field){
			$fields{ $row->{class_id} } = $row;
		}
	}
	
	return \%fields;
}

# Opposite of normalize
sub resolve_value {
	my $self = shift;
	my $class_id = shift;
	my $value = shift;
	my $col = shift;
	
	my $field_order = $Field_to_order->{$col};
	unless (defined $field_order){
		$col =~ s/\_id$//;
		$field_order = $Field_to_order->{$col};
		unless ($field_order){
			$self->log->warn('No field_order found for col ' . $col);
			return $value;
		}
	}
	
	if ($Field_order_to_meta_attr->{$field_order}){
		$class_id = 0;
	}
	
	if ($self->meta_info->{field_conversions}->{ $class_id }->{TIME}->{$field_order}){
		return epoch2iso($value * $Time_values->{ $Field_order_to_attr->{$field_order} });
	}
	elsif ($self->meta_info->{field_conversions}->{ $class_id }->{IPv4}->{$field_order}){
		#$self->log->debug("Converting $value from IPv4");
		return inet_ntoa(pack('N', $value));
	}
	elsif ($self->meta_info->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		#$self->log->debug("Converting $value from proto");
		return exists $Inverse_proto_map->{ $value } ? $Inverse_proto_map->{ $value } : $value;
	}
	elsif ($self->meta_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $self->meta_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		my @arr = $value =~ /(\d{2})(\d{2})/;
		if (@arr){
			return unpack('A*', pack('c*', @arr));
		}
		else {
			return $value;
		}
	}
	elsif ($Field_order_to_attr->{$field_order} eq 'class_id'){
		return $self->meta_info->{classes_by_id}->{$class_id};
	}
	else {
		#apparently we don't know about any conversions
		#$self->log->debug("No conversion for $value and class_id $class_id");
		return $value; 
	}
}

sub resolve_field_permissions {
	my ($self, $user) = @_;
	return if $user->permissions->{resolved}; # allows this to be idempotent
	
	if ($user->is_admin){
		$user->permissions->{fields} = {};
		$user->permissions->{resolved} = 1;
		return;
	}
	
	my %permissions;
	foreach my $field (keys %{ $user->permissions->{fields} }){
		foreach my $value (@{ $user->permissions->{fields}->{$field} }){
			my $field_infos = $self->get_field($field);
											
			# Set attributes for searching
			foreach my $class_id (keys %{ $field_infos }){
				my $attr_name = $Field_order_to_attr->{ $field_infos->{$class_id}->{field_order} };
				my $field_name = $Field_order_to_field->{ $field_infos->{$class_id}->{field_order} };
				my $attr_value = $value;
				$attr_value = $self->normalize_value($class_id, $attr_value, $field_infos->{$class_id}->{field_order});
				
				$permissions{$class_id} ||= [];
				push @{ $permissions{$class_id} }, 
					{ name => $field, attr => [ $attr_name, $attr_value ], field => [ $field_name, $value ] };
			}
		}
	}
	$user->permissions->{fields} = \%permissions;
	$user->permissions->{resolved} = 1;
}

1;