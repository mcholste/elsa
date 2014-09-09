package Info::Windows;
use Moose;
use Data::Dumper;
extends 'Info';
has 'eventid' => (is => 'rw', isa => 'Int', required => 1);

sub BUILDARGS {
	my ($class, %args) = @_;
	
	$args{eventid} = $args{data}->{eventid};
	
	return \%args;
}

sub BUILD {
	my $self = shift;
	if ($self->conf->get('info/windows/url_templates')){
		foreach my $template (@{ $self->conf->get('info/windows/url_templates') }){
			push @{ $self->urls }, sprintf($template, $self->eventid);
		}
	}
	# New-style arbitrary field/url pairs expecting { "field": "myfield", "template": "http://somewhere/%s" }
	my $conf = $self->conf->get('info/windows/templates');
	if ($conf){
		foreach my $hash (@{ $conf }){
			next unless $self->data->{ $hash->{field} };
			my $value = $self->data->{ $hash->{field} };
			push @{ $self->urls }, sprintf($hash->{template}, $value);
		}
	}
}

1;