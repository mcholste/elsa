package View::Mobile;
use Moose;
extends 'View';
with 'Fields';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Scalar::Util;
use Date::Manip;

our $Summary_fields = [ qw(site sig_msg eventid subject srcip dstip) ];

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/html');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->controller->clear_warnings;
	
	my $method = $self->extract_method($req->request_uri);
	$method ||= 'index';
	if ($method eq 'm'){
		$method = 'index';
	}
	$self->controller->log->debug('method: ' . $method);
	
	# Make sure private methods can't be run from the web
	if ($method =~ /^\_/){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	
	my $args = $req->parameters->as_hashref;
	if ($self->session->get('user')){
		$args->{user} = $self->controller->get_stored_user($self->session->get('user'));
	}
	else {
		$args->{user} = $self->controller->get_user($req->user);
	}
	unless ($self->can($method)){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	
	if ($method eq 'query'){
		$res->body([$self->query($req, $args)]);
	}
	else {
		$res->body([$self->index($req, $args)]);
	}
	
	$res->finalize();
}

sub index {
	my $self = shift;
	my $req = shift;
	my $args = shift;
	
	return $self->_get_headers() . $self->_get_index_body($args);
}

sub _get_headers {
	my $self = shift;

	my $dir = $self->path_to_inc;
	my $title = $self->title;

	my $HTML =<<"EOHTML"
<html>
<head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=0"/>
        <title>$title</title>
        <link rel="stylesheet" href="$dir/inc/iui/iui.css" type="text/css" />
        <link rel="stylesheet" href="$dir/inc/iui/t/default/default-theme.css"  type="text/css"/>
        <link rel="apple-touch-icon" href="$dir/inc/icon.png">
        <link rel="apple-touch-startup-image" href="$dir/inc/startup.png">
        <meta name="apple-mobile-web-app-capable" content="yes">
        <script type="application/x-javascript" src="$dir/inc/iui/iui.js"></script>
</head>
EOHTML
;

	return $HTML;

}

sub _get_index_body {
	my $self = shift;
	my $args = shift;
			
	my $edit = '';
	if ($args->{edit}){
		$edit = 'YAHOO.ELSA.editCharts = true;';
	}
	
	$self->controller->log->debug('args: ' . Dumper($args));	
	my $dir = $self->path_to_inc;
	
	my $start = epoch2iso(time() - (86400 * $self->controller->conf->get('default_start_time_offset')));
	my $end = epoch2iso(time);
	
	my $HTML =<<"EOHTML"
<body>
	<div class="toolbar">
		<h1 id="pageTitle"></h1>
		<a id="backButton" class="button" href="#"></a>
	</div>

	<ul id="main" title="ELSA" selected="true">
		<form id="search" title="Search" class="dialog" target="_self" name="query_form" action="$dir/m/query" method="POST">
		<fieldset>
            <h1>Search</h1>
            <!--<a class="button leftButton" type="cancel">Cancel</a>
            <a class="button blueButton" type="submit">Search</a>-->

            <label for="keyword">Search:</label>
            <input id="q" type="text" name="query_string" />
            <label for="keyword">Start:</label>
            <input id="start" type="text" name="start" value="$start" size="20" />
            <label for="keyword">End:</label>
            <input id="end" type="text" name="end" value="$end" />
		</fieldset>
		<a class="whiteButton" href="javascript:query_form.submit()">Search</a>
    </form>
	</ul>

	<div id="results" title="Results">
		<div id="results_container"></div>
	</div>

	<div id="row" title="Log" class="panel">
		<div id="log" />
	</div>
</body>
</html>
EOHTML
;
	return $HTML;
}

sub query {
	my $self = shift;
	my $req = shift;
	my $args = shift;
	
	if (exists $args->{start}){
		$args->{start} = UnixDate(ParseDate(delete $args->{start}), '%s');
		$self->controller->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
	}
	else {
		$args->{start} = (time() - (86400 * $self->controller->conf->get('default_start_time_offset')));
	}
	if (exists $args->{end}){
		$args->{end} = UnixDate(ParseDate(delete $args->{end}), '%s');
		$self->controller->log->trace('set end_time to ' . (scalar localtime($args->{end_time})));
	}
	else {
		$args->{end} = time;
	}
	
	my $ret;
	eval {
		$self->controller->freshen_db;
		$ret = $self->controller->query($args);
		unless ($ret){
			$ret = { error => $self->controller->last_error };
		}
	};
	if ($@){
		my $e = $@;
		$self->controller->log->error($e);
	}
		
	my $HTML = <<"EOHTML"
<body>
    <div class="toolbar">
		<h1 id="pageTitle">Results</h1>
		<a id="backButton" class="button" href="#"></a>
		<a class="button blueButton" href="../m/" target="_self">New Search</a>
	</div>
EOHTML
;

	my %summarized;
	foreach my $row (@{ $ret->results->results }){
		my $drilldown_template = '<div id="query_result_%d" class="panel" title="Result"><fieldset>%s</fieldset></div>';
		my $drilldown_html = '';
		foreach my $key (qw(timestamp class host program msg)){
			$drilldown_html .= '<div class="row"><label>' . $key . '</label><span>' . $row->{$key} .'</span></div>';
		}
		foreach my $field (@{ $row->{_fields} }){
			$drilldown_html .= '<div class="row"><label>' . $field->{field} . '</label><span>' . $field->{value} .'</span></div>';
		}
		$HTML .= sprintf($drilldown_template, $row->{id}, $drilldown_html);	
		
		my $summary_field;
		FIELD_LOOP: foreach my $candidate_field (@$Summary_fields){
			foreach my $field (@{ $row->{_fields} }){
				if ($field->{field} =~ $candidate_field){
					$summary_field = $field->{value};
					last FIELD_LOOP;
				}
			}
		}
		unless ($summary_field){
			$summary_field = join('', split('', $row->{msg}, 30));
		}
		my $summary_key = join(' ', $row->{class}, $summary_field);
		$summarized{$summary_key} ||= [];
		push @{ $summarized{$summary_key} }, $row;
	}
	
	my $summary_html = '';
	my $summary_group_template = '<ul id="result_group_%1$s" title="%1$s">%2$s</ul>';
	my $summary_template = '<ul id="results" title="' . $ret->query_string . '" selected="true"><li class="group">' .
		' Total: ' . $ret->results->records_returned . '/' . $ret->results->total_records . ', ' . $ret->time_taken . ' ms</li>%s</ul>';
	foreach my $summary_key (sort { scalar @{ $summarized{$b} } <=> scalar @{ $summarized{$a} } } keys %summarized){
		my $summary_key_id = $summary_key;
		$summary_key_id =~ s/[^\w]/\_/g;
		$summary_html .= '<li><a href="#result_group_' . $summary_key_id . '">' . $summary_key 
			. ' (' . scalar @{ $summarized{$summary_key} } . ') '
			. '</label></div>';
		
		my $individual_html = '';
		foreach my $row (@{ $summarized{$summary_key} }){
			$individual_html .= '<li><a href="#query_result_' . $row->{id} . '">' .  $row->{timestamp} . '</a></li>';
		}
		$HTML .= sprintf($summary_group_template, $summary_key_id, $individual_html);
	}
	$HTML .= sprintf($summary_template, $summary_html);
	


$HTML .= <<EOHTML
		
</body>
</html>	
EOHTML
;
	
	return $self->_get_headers() . $HTML;
}

1;