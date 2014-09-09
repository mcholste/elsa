package YUI;
use Moose;

has 'js_components' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [ qw(
	yahoo
	event
	connection
	datasource
	dom
	autocomplete
	element
	dragdrop
	container
	menu
	button
	calendar
	json
	charts
	paginator
	datatable
	get
	logger
	selector
	slider
	tabview
	treeview
	uploader
	swf
)]});

has 'css_components' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [ qw(
	paginator
	datatable
	container
	calendar
	button
	menu
	autocomplete
	tabview 
)]});

has 'base_css' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [ qw(
	fonts
)]});

has 'modifier' => (is => 'rw', isa => 'Str', required => 1, default => '-min' );
has 'version' => (is => 'rw', isa => 'Str', required => 1, default => '2.8.1' );
has 'local' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'ssl' => (is => 'rw', isa => 'Int', required => 1, default => 1);

sub css {
	my $self = shift;
	my $path_to_inc = shift;
	$path_to_inc ||= '';
	
	if ($self->local){
		#return '<link rel="stylesheet" type="text/css" href="' . $self->local . 'yui' . '-' . $self->version . $self->modifier . '.css' . '">';
		my $yui_css_base_template = '<link rel="stylesheet" type="text/css" href="%1$s/yui/build/%2$s/%2$s%3$s.css">' . "\n";
		my $yui_css_base = '';
		foreach my $yui_base_css_item (@{$self->base_css}){
			# CSS only has -min modifier
			my $modifier = '';
			if ($self->modifier eq '-min'){
				$modifier = '-min';
			}
			$yui_css_base .= sprintf($yui_css_base_template, $path_to_inc . $self->local, $yui_base_css_item, $modifier);
		}
		
		my $yui_css_template = '<link rel="stylesheet" type="text/css" href="%1$s/yui/build/%2$s/assets/skins/sam/%2$s.css">' . "\n";
		my $yui_css = '';
		foreach my $yui_css_component (@{$self->css_components}){
			$yui_css .= sprintf($yui_css_template, $path_to_inc . $self->local, $yui_css_component);
		}
		return $yui_css_base . "\n" . $yui_css;
	}
	elsif ($self->ssl){
		my $yui_css_base_template = '<link rel="stylesheet" type="text/css" href="https://ajax.googleapis.com/ajax/libs/yui/%1$s/build/%2$s/%2$s%3$s.css">' . "\n";
		my $yui_css_base = '';
		foreach my $yui_base_css_item (@{$self->base_css}){
			# CSS only has -min modifier
			my $modifier = '';
			if ($self->modifier eq '-min'){
				$modifier = '-min';
			}
			$yui_css_base .= sprintf($yui_css_base_template, $self->version, $yui_base_css_item, $modifier);
		}
		
		my $yui_css_template = '<link rel="stylesheet" type="text/css" href="https://ajax.googleapis.com/ajax/libs/yui/%1$s/build/%2$s/assets/skins/sam/%2$s.css">' . "\n";
		my $yui_css = '';
		foreach my $yui_css_component (@{$self->css_components}){
			$yui_css .= sprintf($yui_css_template, $self->version, $yui_css_component);
		}
		return $yui_css_base . "\n" . $yui_css;
	}
	else {
		my $yui_css_base_template = '%1$s/build/%2$s/%2$s.css&';
		my $yui_css_base = '';
		foreach my $yui_base_css_item (@{$self->base_css}){
			$yui_css_base .= sprintf($yui_css_base_template, $self->version, $yui_base_css_item);
		}
		
		my $yui_css_template = '%1$s/build/%2$s/assets/skins/sam/%2$s.css&';
		my $yui_css = 'http://yui.yahooapis.com/combo?' . $yui_css_base . '&';
		foreach my $yui_css_component (@{$self->css_components}){
			$yui_css .= sprintf($yui_css_template, $self->version, $yui_css_component, $self->modifier);
		}
		return '<link rel="stylesheet" type="text/css" href="' . $yui_css . '">';
	}
}

sub js {
	my $self = shift;
	my $path_to_inc = shift;
	$path_to_inc ||= '';
	if ($self->local){
		#return '<script type="text/javascript" src="' . $self->local . 'yui' . '-' . $self->version . $self->modifier . '.js' . '"></script>';
		my $yui_js_template = '<script type="text/javascript" src="%1$s/yui/build/%2$s/%2$s%3$s.js"></script>' . "\n";
		my $yui_js = '';
		foreach my $yui_component (@{$self->js_components}){
			$yui_js .= sprintf($yui_js_template, $path_to_inc . $self->local, $yui_component, $self->modifier);
		}
		return $yui_js;
	}
	elsif ($self->ssl){
		my $yui_js_template = '<script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/yui/%1$s/build/%2$s/%2$s%3$s.js"></script>' . "\n";
		my $yui_js = '';
		foreach my $yui_component (@{$self->js_components}){
			$yui_js .= sprintf($yui_js_template, $self->version, $yui_component, $self->modifier);
		}
		return $yui_js;
	}		
	else {
		my $yui_js_template = '%1$s/build/%2$s/%2$s%3$s.js&';
		my $yui_js = 'http://yui.yahooapis.com/combo?';
		foreach my $yui_component (@{$self->js_components}){
			$yui_js .= sprintf($yui_js_template, $self->version, $yui_component, $self->modifier);
		}
		return '<script type="text/javascript" src="' . $yui_js . '"></script>';
	}
}

1;