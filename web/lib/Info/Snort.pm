package Info::Snort;
use Moose;
use Data::Dumper;
extends 'Info';
has 'sid' => (is => 'rw', isa => 'Int', required => 1);
has 'gid' => (is => 'rw', isa => 'Int');
has 'rev' => (is => 'rw', isa => 'Int');

sub BUILDARGS {
	my ($class, %args) = @_;
	$args{data}->{sig_sid} =~ /(\d+):(\d+):(\d+)/;
	$args{gid} = $1;
	$args{sid} = $2;
	$args{rev} = $3;
	return \%args;
}

sub BUILD {
	my $self = shift;
	if ($self->conf->get('info/snort/url_templates')){
		foreach my $template (@{ $self->conf->get('info/snort/url_templates') }){
			push @{ $self->urls }, sprintf($template, $self->sid);
		}
	}
	# New-style arbitrary field/url pairs expecting { "field": "myfield", "template": "http://somewhere/%s" }
	my $conf = $self->conf->get('info/snort/templates');
	if ($conf){
		foreach my $hash (@{ $conf }){
			next unless $self->data->{ $hash->{field} };
			my $value = $self->data->{ $hash->{field} };
			push @{ $self->urls }, sprintf($hash->{template}, $value);
		}
	}
}

1;