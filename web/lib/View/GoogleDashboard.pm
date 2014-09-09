package View::GoogleDashboard;
use Moose;
extends 'View';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Module::Pluggable require => 1, search_path => [qw(Dashboard)];
use JSON;
use Plack::Middleware::Auth::Basic;
use Date::Manip;
use Socket;
use AnyEvent;
use Try::Tiny;
use Ouch qw(:trytiny);

our $Default_width = 1000;

has 'system_dashboards' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
	_system => {
		title => 'System Dashboard',
		node_charts => [
			{
				x => 0,
				y => 0,
				chart_type => 'ColumnChart',
				chart_options => {
					title => 'Events per Time',
					isStacked => 1,
				},
				query => 'datasource:_node_stats node:%s',
				label => 'Events %s',
			},
		],
		charts => [
			{
				x => 0,
				y => 1,
				chart_type => 'PieChart',
				chart_options => {
					title => 'Events per Host',
				},
				query => 'datasource:_system_event_rates groupby:host',
				label => 'Host',
			},
			{
				x => 1,
				y => 1,
				chart_type => 'PieChart',
				chart_options => {
					title => 'Events per Class',
				},
				query => 'datasource:_system_event_rates groupby:class',
				label => 'Class',
			},
			{
				x => 0,
				y => 2,
				chart_type => 'ColumnChart',
				chart_options => {
					title => 'Queries per Time',
					logScale => 1,
				},
				query => 'datasource:_system_web_queries_count',
				label => 'Queries',
			},
			{
				x => 0,
				y => 2,
				query => 'datasource:_system_web_queries_time',
				label => 'Query Time (ms)',
			},
			{
				x => 0,
				y => 3,
				chart_type => 'PieChart',
				chart_options => {
					title => 'Queries per User',
				},
				query => 'datasource:_system_web_queries_count groupby:username',
				label => 'User',
			},
			{
				x => 1,
				y => 3,
				chart_type => 'PieChart',
				chart_options => {
					title => 'Queries per User by Time',
				},
				query => 'datasource:_system_web_queries_time groupby:username',
				label => 'User',
			},
		]
	},
}});

sub call {
	my ($self, $env) = @_;
	
	my $req = Plack::Request->new($env);
	my $args = $req->parameters->as_hashref;
	my $res = $req->new_response(200); # new Plack::Response
	my $ret = [];
	
	# If we don't have a nonblocking web server (Apache), we need to have an overarching blocking recv
	my $cv;
	if (not $env->{'psgi.nonblocking'}){
		$cv = AnyEvent->condvar;
	}
	
	return sub {
		my $write = shift;
		
		try {
		    $self->session(Plack::Session->new($env));
			$res->content_type('text/html');
			$res->header('Access-Control-Allow-Origin' => '*');
			$self->path_to_inc('../');
			
			my $dashboard_name = $self->extract_method($req->request_uri);
			$self->controller->log->debug('method: ' . $dashboard_name);
			
			my $user = $self->controller->get_user($req->user);
			if ($user){
				$self->session->set('user', $user->freeze);
				$self->session->set('user_info', $user->TO_JSON);
			}
			
			$args->{alias} = $dashboard_name;
			
			if ($req->request_uri =~ /[\?\&]edit[=]?/){
				$args->{edit} = 1;
				$self->controller->log->trace('edit mode');
			}
			
			#$self->controller->log->debug('dashboard args: ' . Dumper($args));
			if (exists $args->{start}){
				$args->{start_time} = UnixDate(ParseDate($args->{start}), '%s');
				$self->controller->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
			}
			else {
				$args->{start_time} = (time() - (86400*7));
			}
			if (exists $args->{end}){
				$args->{end_time} = UnixDate(ParseDate($args->{end}), '%s');
				$self->controller->log->trace('set end_time to ' . (scalar localtime($args->{end_time})));
			}
			else {
				$args->{end_time} = time;
			}
			
			my $time_units = {
				seconds => { groupby => 'timestamp', multiplier => 1 },
				minutes => { groupby => 'minute', multiplier => 60 },
				hours => { groupby => 'hour', multiplier => 3600 },
				days => { groupby => 'day', multiplier => 86400 },
				months => { groupby => 'month', multiplier => 2592000 },
				years => { groupby => 'year', multiplier => 946080000 },
			};
			
			$args->{groupby} = 'hour';
			
			foreach my $arg (keys %$args){
				if (exists $time_units->{ $arg }){
					$args->{groupby} = $time_units->{ $arg }->{groupby};
					if ($args->{$arg}){
						if ($args->{start}){
							$args->{end_time} = ($args->{start_time} + ($time_units->{ $arg }->{multiplier} * int($args->{$arg})));
							$self->controller->log->trace('set end_time to ' . (scalar localtime($args->{end_time})));
						}
						else {
							$args->{start_time} = ($args->{end_time} - ($time_units->{ $arg }->{multiplier} * int($args->{$arg})));
							$self->controller->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
						}
					}
					last;
				}
			}
			foreach my $plural_unit (keys %$time_units){
				if ($time_units->{$plural_unit}->{groupby} eq $args->{groupby}){
					$args->{limit} = ($args->{end_time} - $args->{start_time}) / $time_units->{$plural_unit}->{multiplier};
				}
			}
		
		
			$self->controller->freshen_db;
			my ($query, $sth);
			
			$args->{user} = $user;
			$args->{dashboard_name} = $dashboard_name;
			if ($self->controller->conf->get('dashboard_width')){
				$args->{width} = $self->controller->conf->get('dashboard_width');
			}
			else {
				$args->{width} = $Default_width;
			}
			
			if(exists $self->system_dashboards->{ $args->{alias} }){
				unless ($args->{user}->is_admin){
					$res->status(401);
					$res->body('Unauthorized');
					$cv and $cv->send;
					return;
				}
				$args->{id} = 1;
				$args->{title} = $self->system_dashboards->{ $args->{alias} }->{title};
				$args->{_system_dashboard} = $self->_generate_system_dashboard($args->{alias});
			}
			else {
				$query = 'SELECT dashboard_id, dashboard_title, alias, auth_required FROM v_dashboards WHERE alias=? ORDER BY x,y';
				$sth = $self->controller->db->prepare($query);
				$sth->execute($dashboard_name);
				my $row = $sth->fetchrow_hashref;
				die('dashboard ' . $dashboard_name . ' not found or not authorized') unless $row;
				$args->{id} = $row->{dashboard_id};
				$args->{title} = $row->{dashboard_title};
				$self->title($args->{title});
				$args->{alias} = $row->{alias};
				$args->{auth_required} = $row->{auth_required};
				unless ($self->controller->_is_permitted($args)){
					$res->status(401);
					die('Unauthorized');
				}
			}
				
			$ret = $self->controller->_get_rows($args);
			
			delete $args->{user};
			$self->controller->log->debug('data: ' . Dumper($ret));
			$res->body([$self->index($req, $args, $ret, sub {
				my $body = shift;
				$res->body($body);
				$write->($res->finalize());
				$cv and $cv->send;
			})]);
		}
		catch {
			my $e = shift;
			$self->controller->log->error($e);
			$res->body([encode_utf8($self->controller->json->encode({error => $e}))]);
			$write->($res->finalize());
			$cv and $cv->send;
		}
	};
}

sub index {
	my $self = shift;
	my $req = shift;
	my $args = shift;
	my $queries = shift;
	my $cb = shift;
	my $ret;
	$self->get_headers(1, sub {
		my $headers = shift;
		$cb->($headers . $self->_get_index_body($args, $queries));
	});
}

sub _get_index_body {
	my $self = shift;
	my $args = shift;
	my $queries = shift;
		
	my $edit = '';
	if ($args->{edit}){
		$edit = 'YAHOO.ELSA.editCharts = true;';
	}
	
	my $refresh = 'YAHOO.ELSA.dashboardRefreshInterval = false;';
	if ($args->{refresh} and int($args->{refresh}) >= 5){
		$refresh = 'YAHOO.ELSA.dashboardRefreshInterval = ' . (1000 * int($args->{refresh})) . ';';
	}
		
	my $json = $self->controller->json->encode($queries);
	my $dir = $self->path_to_inc;
	my $defaults = $self->controller->json->encode({
		groupby => [$args->{groupby}],
		start => $args->{start_time},
		end => $args->{end_time}
	});
		
	my $yui = new YUI(%{ $self->controller->conf->get('yui') });
	my $yui_css = $yui->css($dir);
	my $yui_js = $yui->js($dir);

	my $HTML =<<"EOHTML"
<!--Load the AJAX API-->
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript" src="$dir/inc/elsa.js" ></script>
<script type="text/javascript" src="$dir/inc/dashboard.js" ></script>
$yui_css
$yui_js
<link rel="stylesheet" type="text/css" href="$dir/inc/custom.css" />
<script>
$edit
$refresh
// Set viewMode for dev/prod
var oRegExp = new RegExp('\\\\Wview=(\\\\w+)');
var aMatches = oRegExp.exec(location.search);
if (aMatches){
	console.log('matched');
	YAHOO.ELSA.viewMode = aMatches[1];
}
YAHOO.ELSA.queryMetaParamsDefaults = $defaults;
YAHOO.ELSA.dashboardParams = {
	id: $args->{id},
	title: '$args->{title}',
	alias: '$args->{alias}',
	container: 'google_charts',
	rows: $json,
	width: $args->{width}
};
			 
// Load the Visualization API and the piechart package.
google.load('visualization', '1.0', {'packages':['corechart', 'charteditor', 'controls']});

YAHOO.util.Event.addListener(window, "load", function(){
	YAHOO.ELSA.initLogger();
	// Set a callback to run when the Google Visualization API is loaded.
	//google.setOnLoadCallback(loadCharts);
	//YAHOO.ELSA.Chart.loadCharts();
	oDashboard = new YAHOO.ELSA.Dashboard($args->{id}, '$args->{title}', '$args->{alias}', $json, 'google_charts');
	if (YAHOO.ELSA.dashboardRefreshInterval){
		//YAHOO.lang.later(YAHOO.ELSA.dashboardRefreshInterval, oDashboard, 'redraw', [], true);
		YAHOO.lang.later(YAHOO.ELSA.dashboardRefreshInterval, location, 'reload', [], true);
	}
});
</script>
</head>

  <body class=" yui-skin-sam">
   <div id="panel_root"></div>
    <div id="google_charts"></div>
  </body>
</html>
EOHTML
;
	return $HTML;
}

sub _generate_system_dashboard {
	my $self = shift;
	my $dashboard = shift;
	
	$self->controller->log->trace('Generating system dashboard ' . $dashboard);
	
	my $ret = { %{ $self->system_dashboards->{$dashboard} } };
	$ret->{charts} = [];
	my $id_counter = 1;
	if ($self->system_dashboards->{$dashboard}->{node_charts}){
		foreach my $chart (@{ $self->system_dashboards->{$dashboard}->{node_charts} }){
			my $clone = { %$chart };
			foreach my $peer (keys %{ $self->controller->conf->get('peers') }){
				$clone->{chart_id} = $id_counter;
				$clone->{query_id} = $id_counter;
				$clone->{query} = sprintf($chart->{query}, $peer);
				$clone->{label} = sprintf($chart->{label}, $peer);
				push @{ $ret->{charts} }, { %$clone };
				$id_counter++;
			}
		}
	}
	foreach my $chart (@{ $self->system_dashboards->{$dashboard}->{charts} }){
		my $clone = { %$chart };
		$clone->{chart_id} = $id_counter;
		$clone->{query_id} = $id_counter;
		push @{ $ret->{charts} }, $clone;
		$id_counter++;
	}
	$self->controller->log->debug('ret: ' . Dumper($ret));
	
	return $ret;
}

1;