#!/usr/bin/env perl
use strict;
use 5.010;
use Data::Dumper;
use Config::JSON;
use Getopt::Std;
use Digest::SHA qw(sha1_hex);
use File::Copy qw(copy);

my %Opts;
getopts('c:n:v:d', \%Opts);

my $config_file = -f '/etc/elsa_web.conf' ? '/etc/elsa_web.conf' : '/usr/local/elsa/web/conf/elsa.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}
elsif ($Opts{c}){
	$config_file = $Opts{c};
}

my $node_config_file = -f '/etc/elsa_node.conf' ? '/etc/elsa_node.conf' : '/usr/local/elsa/node/conf/elsa.conf';
if ($ENV{ELSA_NODE_CONF}){
	$node_config_file = $ENV{ELSA_NODE_CONF};
}
elsif ($Opts{n}){
	$node_config_file = $Opts{n};
}

my $version = $Opts{v} ? $Opts{v} : 'HEAD';

my $Required = {
	HEAD => {
		web => {
			apikeys => [],
			peers => [
				{
					url => 'string',
					username => 'string',
					apikey => 'string',
				},
			],
			meta_db => {
				dsn => 'string',
				username => 'string',
				password => 'string',
			},
			auth => {
				method => sub { _enum($_[1], qw(none local ldap db kerberos)); },
			},
			email => {
				display_address => 'string',
				base_url => 'string',
				subject => 'string',
			},
			yui => \&_validate_yui,
			connectors => [],
			datasources => [],
			transforms => [],
			plugins => [],
			info => [],
			admin_groups => 'array',
			data_db => {
				username => 'string',
				password => 'string',
			},
			admin_email_address => 'string',
			max_concurrent_archive_queries => 'integer',
			schedule_interval => 'integer',
			node_info_cache_timeout => 'integer',
			link_key => 'string',
			query_timeout => 'integer',
			logdir => 'string',
			debug_level => sub { _enum($_[1], qw(ERROR WARN INFO DEBUG TRACE FATAL)) },
			default_start_time_offset => 'integer',
			livetail => {
				poll_interval => 'integer',
				time_limit => 'integer',
			},
		},
		node => {
			database => {
				db => 'string',
				data_db => 'string',
				dsn => 'string',
				username => 'string',
				password => 'string'
			},
			lockfile_dir => 'dir',
			num_indexes => 'integer',
			archive => sub { 
				(exists $_[1]->{table_size} and int($_[1]->{table_size}) > 1000) and 
				((exists $_[1]->{percentage} and int($_[1]->{percentage}) > 0 and int($_[1]->{percentage}) < 100)
				or (exists $_[1]->{percentage} and ($_[1]->{percentage} == 0 or $_[1]->{percentage} eq '0'))
				or (exists $_[1]->{days} and int($_[1]->{days}) > 0)) 
			},
			log_size_limit => sub { if ($_[1] =~ /^(\d{1,2})\%/){ return $1 > 0 and $1 < 100 } else { return int($_[1]); } },
			sphinx => {
				indexer => 'file',
				allowed_temp_percent => 'percent',
				allowed_mem_percent => 'percent',
				host => 'string',
				port => 'integer',
				mysql_port => 'integer',
				config_file => 'file',
				index_path => 'dir',
				index_interval => 'integer',
				perm_index_size => 'integer',
				stopwords => {
					file => 'string', #optional
					top_n => 'integer',
					interval => 'integer',
					whitelist => 'array',
				},
				pid_file => 'string',
			},
			logdir => 'dir',
			num_log_readers => 'integer',
			debug_level => sub { _enum($_[1], qw(ERROR WARN INFO DEBUG TRACE FATAL)) },
			buffer_dir => 'dir',
#			mysql_dir => 'dir',
			log_parse_errors => 'integer',
			stats => {
				retention_days => 'integer',
			},
			min_expected_hosts => 'integer',
		}		
	}
};

my $Additions = {
	'apikeys' => sub {
		my $fails = shift;
		my $conf = shift;
		say 'Creating local user and API key.';
		my $temp_api_key;
		for (1..1_000){
			$temp_api_key .= rand(10);
		}
		$temp_api_key = sha1_hex($temp_api_key);
		$conf->set('apikeys', { 'elsa' => $temp_api_key });
		$conf->set('peers/127.0.0.1/url', 'http://127.0.0.1/');
		$conf->set('peers/127.0.0.1/username', 'elsa');
		$conf->set('peers/127.0.0.1/apikey', $temp_api_key);
		
		# Create peer entries for all current nodes
		foreach my $node (keys %{ $conf->get('nodes') }){
			$conf->set('peers/' . $node . '/url', 'http://' . $node . '/');
			$conf->set('peers/' . $node . '/username', 'elsa');
			$conf->set('peers/' . $node . '/apikey', $temp_api_key);
		}
		say 'IMPORTANT!  You will need to install the API key on any peers (nodes) this server needs to talk to!' . "\n" .
		'Add the following configuration to the /etc/elsa_web.conf file on each remote peer this server talks to: ' . "\n" .
		'"apikeys": { "username": "elsa", "apikey": "' . $temp_api_key . '" }' . "\n" .
		'You will need to update this host /etc/elsa_web.conf with the correct peers/<peer IP>/apikey for the remote node if you do not make it the same apikey as this host.';
		
		# Remove any pending tasks for peers/127.0.0.1
		for (my $i = 0; $i < @$fails; $i++){
			if ($fails->[$i]->{keypath} =~ /^peers\/127\.0\.0\.1/ or $fails->[$i]->{keypath} eq 'peers'){
				splice(@$fails, $i, 1);
				warn 'removed $i ' . $i;
			}
		}
		return 1;
	},
	'sphinx/stopwords' => sub {
		my $fails = shift;
		my $conf = shift;
		say 'Creating dummy stopwords config entry';
		$conf->set('sphinx/stopwords', { file => '/usr/local/etc/sphinx_stopwords.txt', top_n => 0, interval => 0, whitelist => [] });
	},
	'data_db' => sub {
		my $fails = shift;
		my $conf = shift;
		say 'Creating data_db entry';
		$conf->set('data_db', { db => 'syslog', username => 'elsa', password => 'biglog' });
	},
		
#	'mysql_dir' => sub {
#		my $fails = shift;
#		my $conf = shift;
#		my $data_dir = $conf->get('sphinx/index_path');
#		$data_dir =~ /^(\/[^\/]+)/;
#		$data_dir = $1 . '/elsa/mysql';
#	},
};

die('Invalid version given: ' . $version) unless exists $Required->{$version};
my $web_config = new Config::JSON($config_file) or die('elsa_web.conf not found, please run install.sh web');
my $fails = [];
_validate($Required->{$version}->{web}, $web_config, '', $fails);
my $node_config = new Config::JSON($node_config_file) or die('elsa_node.conf not found, please run install.sh node');
_validate($Required->{$version}->{node}, $node_config, '', $fails);
#print Dumper($fails);
my $backed_up;
for (my $i = 0; $i < @$fails; $i++){
	my $fail = $fails->[$i];
	if ($fail->{type} eq 'add'){
		unless ($backed_up){
			$backed_up = $fail->{conf}->pathToFile() . '.bak.' . CORE::time();
			copy($fail->{conf}->pathToFile(), $backed_up);
			say "Backed up config to $backed_up";
		}
		if (_add($fails, $i)){
			say 'Successfully added ' . $fail->{keypath};
			splice(@$fails, $i, 1);
			$i--;
		}
		else {
			say 'Failed to add ' . $fail->{keypath};
		}
	}
	else {
		say 'Invalid configuration: ' . $fail->{type} . ': ' . $fail->{keypath};
	}
}
if (scalar @$fails){
	say 'Config is invalid: ' . join(',', map { $_->{keypath} } @$fails);
	exit 1;
}
else {
	say 'Config is valid.';
	exit;
}


sub _validate {
	my ($required, $config_json, $path, $fails) = @_;
	my $conf = $config_json->get($path);
	foreach my $key (sort keys %$required){
		my $keypath = $path ? $path . '/' . $key : $key;
		unless (exists $conf->{$key}){
			say 'Missing required item ' . $keypath;
			push @$fails, { type => 'add', keypath => $keypath, conf => $config_json };
			next;
		}
		if (ref($required->{$key}) eq 'ARRAY'){ # signifies that this config is for children
			unless (ref($conf->{$key}) eq 'HASH'){
				say 'Invalid hash child: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
				next;
			}
			my $orig_keypath = $keypath;
			foreach my $subkey (keys %{ $conf->{$key} }){
				next unless $required->{$key}->[0];
				$keypath = $orig_keypath;
				$keypath .= '/' . $subkey;
				unless (_validate($required->{$key}->[0], $config_json, $keypath, $fails)){ #$conf->{$key}->{$subkey})){
					say 'Invalid hash child: ' . $keypath;
					push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
				}
			}
		}
		elsif (ref($required->{$key}) eq 'HASH'){
			unless (ref($conf->{$key}) eq 'HASH'){
				say 'Invalid hash key: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
				next;
			}
			unless (_validate($required->{$key}, $config_json, $keypath, $fails)){ #$conf->{$key})){
				say 'Invalid hash key: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
			
		}
		elsif (ref($required->{$key}) eq 'CODE'){
			unless($required->{$key}->($required->{$key}, $conf->{$key})){
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
		elsif ($required->{$key} eq 'string'){
			unless (defined $conf->{$key}){
				say 'Invalid string: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
		elsif ($required->{$key} eq 'integer'){
			unless (int($conf->{$key}) or $conf->{$key} == 0){
				say 'Invalid integer: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
		elsif ($required->{$key} eq 'array'){
			unless (ref($conf->{$key}) eq 'ARRAY'){
				say 'Invalid array: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
		elsif ($required->{$key} eq 'dir'){
			unless (_dir($conf->{$key})){
				say 'Invalid dir: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
		elsif ($required->{$key} eq 'file'){
			unless (_file($conf->{$key})){
				say 'Invalid dir: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
		elsif ($required->{$key} eq 'percent'){
			unless (_percent($conf->{$key})){
				say 'Invalid percent: ' . $key;
				push @$fails, { type => 'invalid', keypath => $keypath, conf => $config_json };
			}
		}
	}
	return { ok => 1 };
}

sub _validate_yui {
	my ($required, $conf) = @_;
	if (not exists $conf->{local}){
		# there must be a version given
		unless ($conf->{version}){
			say 'No version given for yui config';
			return 0;
		}
	}
	elsif (not $conf->{local}){
		say 'Invalid yui/local';
		return 0;
	}
	return 1;
}

sub _enum {
	my ($value, @list) = @_;
	my %valid = map { $_ => 1 } @list; 
	return exists $valid{$value}; 
}

sub _dir { return -d $_[0]; }
sub _file { return -f $_[0]; }
sub _percent { return $_[0] > 0 and $_[0] < 100 }

sub _add {
	my $fails = shift;
	my $i = shift;
	my $fail = $fails->[$i];
	if (exists $Additions->{ $fail->{keypath} }){
		return $Additions->{ $fail->{keypath} }->($fails, $fail->{conf});
	}
	else {
		return 0;
	}
}
