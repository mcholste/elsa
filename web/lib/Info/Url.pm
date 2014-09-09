package Info::Url;
use Moose;
use Data::Dumper;
extends 'Info';

sub BUILD {
	my $self = shift;
	if ($self->conf->get('info/url/url_templates')){
		foreach my $template (@{ $self->conf->get('info/url/url_templates') } ){
			push @{ $self->urls }, sprintf($template, $self->data->{site});
		}
	}
	# New-style arbitrary field/url pairs expecting { "field": "myfield", "template": "http://somewhere/%s" }
	my $conf = $self->conf->get('info/url/templates');
	if ($conf){
		foreach my $hash (@{ $conf }){
			next unless $self->data->{ $hash->{field} };
			my $value = $self->data->{ $hash->{field} };
			push @{ $self->urls }, sprintf($hash->{template}, $value);
		}
	}
}

1;