package Forwarder::SSH;
use Moose;
use Data::Dumper;
use Net::OpenSSH;
extends 'Forwarder';

has 'dir' => (is => 'rw', isa => 'Str', required => 1);
has 'ssh' => (is => 'rw', isa => 'Net::OpenSSH', required => 1);

sub identifiers { [ 'dir', 'host' ] };

sub BUILDARGS {
	my ($class, %params) = @_;
	
	my %ssh_opts = %params;
	$ssh_opts{batch_mode} = 1;
	delete $ssh_opts{method};
	delete $ssh_opts{dir};
	my $host = delete $ssh_opts{host};
	$ssh_opts{master_opts} = ['-o' => "Compression=yes,CompressionLevel=9"];
	my $ssh = Net::OpenSSH->new($host, %ssh_opts);
	if ($ssh->error){
		$params{log}->error('Error opening SSH connection to host ' . $host . ': ' . $ssh->error);
		die('Error opening SSH connection to host ' . $host . ': ' . $ssh->error);
	}
	
	return \%params;
}

sub forward {
	my $self = shift;
	
	foreach (@_){
		my $file = $_;
		$self->log->trace('Scpying file ' . $file);
		$self->ssh->scp_put($file, $self->dir . '/');
		if ($self->ssh->error){
			$self->log->error('Error copying ' . $file . ' to host ' . $self->ssh->get_host . ':' . $self->dir . ': ' . $self->ssh->error);
			return 0;
		}
	}
	
	return 1;					
}

__PACKAGE__->meta->make_immutable;

1;