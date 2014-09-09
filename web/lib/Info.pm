package Info;
use Moose;
use Socket;
# Base class for Info plugins
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'data' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'urls' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'plugins' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'summary' => (is => 'rw', isa => 'Str', required => 1, default => '');

sub BUILD {
	my $self = shift;
	if ($self->conf->get('streamdb_urls')){
		my $best_url;
		URL_LOOP: foreach my $url (keys %{ $self->conf->get('streamdb_urls') }){
			my $start_ip_int = unpack('N*', inet_aton($self->conf->get('streamdb_urls')->{$url}->{start}));
			my $end_ip_int = unpack('N*', inet_aton($self->conf->get('streamdb_urls')->{$url}->{end}));
			foreach my $col (qw(srcip dstip ip)){
				if (exists $self->data->{$col}){
					my $ip_int = unpack('N*', inet_aton($self->data->{$col}));
					if ($start_ip_int <= $ip_int and $ip_int <= $end_ip_int){
						$best_url = $url;
						last URL_LOOP;
					}
				}
			}
		}
		if ($best_url){
			push @{ $self->plugins }, 'getStream_' . $best_url;
		}
	}
	elsif ($self->conf->get('streamdb_url')){
		push @{ $self->plugins }, 'getStream';
	}
	
	if ($self->conf->get('pcap_url')){
		push @{ $self->plugins }, 'getPcap';
	}
	if ($self->conf->get('block_url')){
		push @{ $self->plugins }, 'blockIp';
	}
	if ($self->conf->get('moloch_urls')){
		my $best_url;
		URL_LOOP: foreach my $url (keys %{ $self->conf->get('moloch_urls') }){
			my $start_ip_int = unpack('N*', inet_aton($self->conf->get('moloch_urls')->{$url}->{start}));
			my $end_ip_int = unpack('N*', inet_aton($self->conf->get('moloch_urls')->{$url}->{end}));
			foreach my $col (qw(srcip dstip ip)){
				if (exists $self->data->{$col}){
					my $ip_int = unpack('N*', inet_aton($self->data->{$col}));
					if ($start_ip_int <= $ip_int and $ip_int <= $end_ip_int){
						$best_url = $url;
						last URL_LOOP;
					}
				}
			}
		}
		if ($best_url){
			push @{ $self->plugins }, 'getMoloch_' . $best_url;
		}
	}

	return $self;
}

1;

