package Controller;
use Moose;
with 'MooseX::Traits';
with 'Utils';
with 'Fields';
use Data::Dumper;
use Date::Manip;
use AnyEvent;
use DBI;
use MIME::Base64;
use Socket qw(inet_aton inet_ntoa);
use CHI;
use Time::HiRes qw(time);
use Time::Local;
use Module::Pluggable sub_name => 'export_plugins', require => 1, search_path => [ qw(Export) ];
use Module::Pluggable sub_name => 'info_plugins', require => 1, search_path => [ qw(Info) ];
use Module::Pluggable sub_name => 'connector_plugins', require => 1, search_path => [ qw(Connector) ];
use Module::Pluggable sub_name => 'datasource_plugins', require => 1, search_path => [ qw(Datasource) ];
use Module::Pluggable sub_name => 'stats_plugins', require => 1, search_path => [ qw(Stats) ];
use URI::Escape qw(uri_unescape uri_escape);
use Mail::Internet;
use Email::LocalDelivery;
use Carp;
use Log::Log4perl::Level;
use Storable qw(freeze thaw);
use AnyEvent::HTTP;
use Try::Tiny;
use Ouch qw(:trytiny);;
use B qw(svref_2object);
use Hash::Merge::Simple qw(merge);

use User;
use Query;
use QueryParser;
use Results;
use SyncMysql;
#use AsyncMysql;
#use AsyncDB;

our $Scheduled_query_cols = { map { $_ => 1 } (qw(id username query frequency start end connector params enabled last_alert alert_threshold)) };
our $Query_time_batch_threshold = 120;

has 'ldap' => (is => 'rw', isa => 'Object', required => 0);
has 'last_error' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'cache' => (is => 'rw', isa => 'Object', required => 1, default => sub { return CHI->new( driver => 'RawMemory', global => 1) });
has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_warnings' => 'count', 'clear_warnings' => 'clear', 'all_warnings' => 'elements' });
has 'user_agent_name' => (is => 'ro', isa => 'Str', required => 1, default => 'ELSA API');
has 'stat_objects' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

## Create a stats timer for these methods
#around [qw(_sphinx_query _archive_query _external_query _unlimited_sphinx_query)] => sub {
#	my $coderef = shift;
#	my $self = shift;
#	# Find out the original method name by asking the compiler what this coderef was
#	my $method_name = svref_2object($coderef)->GV->NAME;
#	
#	my $start = [ Time::HiRes::time() ];
#	my $ret = $self->$coderef(@_);
#	foreach my $plugin (@{ $self->stat_objects }){
#		my $ret = $plugin->timing(__PACKAGE__ . '::' . $method_name, Time::HiRes::tv_interval($start) * 1000);
#	}
#	
#	return $ret;
#};

sub BUILD {
	my $self = shift;
	
	if ( uc($self->conf->get('auth/method')) eq 'LDAP' ) {
		require Net::LDAP::Express;
		require Net::LDAP::FilterBuilder;
		$self->ldap($self->_get_ldap());
	}
		
	# init plugins
	$self->export_plugins();
	$self->info_plugins();
	$self->connector_plugins();
	$self->datasource_plugins();
	$self->stats_plugins();
	
	foreach my $plugin_name ($self->stats_plugins()){
		my $plugin = $plugin_name->new(conf => $self->conf);
		push @{ $self->stat_objects }, $plugin;
	}
	
	return $self;
}

sub add_warning {
	my $self = shift;
	my $code = shift;
	my $errstr = shift;
	my $data = shift;
	
	push @{ $self->warnings }, new Ouch($code, $errstr, $data);
}

sub _get_ldap {
	my $self = shift;
	my $ldap = new Net::LDAP::Express(
		host        => $self->conf->get('ldap/host'),
		bindDN      => $self->conf->get('ldap/bindDN'),
		bindpw      => $self->conf->get('ldap/bindpw'),
		base        => $self->conf->get('ldap/base'),
		searchattrs => [ $self->conf->get('ldap/searchattrs') ],
	);
	unless ($ldap) {
		$self->log->error('Unable to connect to LDAP server');
		return;
	}
	return $ldap;
}

sub get_user {
	my $self = shift;
	my $username = shift;
	
	my $user = User->new(username => $username, conf => $self->conf);
	$self->resolve_field_permissions($user);
	return $user;
}

sub get_stored_user {
	my $self = shift;
	my $user_info = shift;
	
	my ($class, $params) = User->thaw($user_info);
	return $class->new(%$params);
}

sub get_user_info {
	my $self = shift;
	my $user_info = shift;
	
	my ($class, $params) = User->thaw($user_info);
	return $params;
	
}

sub get_saved_result {
	my $cb = pop(@_);
	my ($self, $args) = @_;
	
	$self->result($args, $cb);
}

sub result {
	my $cb = pop(@_);
	my ($self, $args) = @_;
	
	if ($args->{user} and not $args->{hash}){
		unless ($args->{user}){
			throw(401, 'Unauthorized');
		}
	}
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer that has a foreign qid
	$query = 'SELECT * FROM foreign_queries WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	my %peers;
	while (my $row = $sth->fetchrow_hashref){
		$peers{ $row->{peer} } = $row->{foreign_qid};
	}
	
	$self->log->trace('Foreign query results on peers ' . Dumper(\%peers));
	
	my %results;
	my %stats;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { 
		
		$self->log->debug('stats: ' . Dumper(\%stats));
		
		my $complete = 0;
		foreach my $peer (keys %results){
			if ($results{$peer}){
				$complete++;
			}
		}
		if ($complete == scalar keys %results){
			$stats{overall} = (time() - $overall_start);
			$self->log->debug('merging: ' . Dumper(\%results));
			my $overall_final = merge values %results;
			$cb->($overall_final);
		}
		else {
			$cb->({ incomplete => { %results } });
		}
	});
	
	$self->local_result($args, sub {
		my $raw_results = shift;
		if ($raw_results){
			$results{'127.0.0.1'} = $raw_results;
		}
		else {
			$results{'127.0.0.1'} = 0;
		}
		$self->log->debug('local_results: ' . Dumper(\%results));
	});
	
	foreach my $peer (sort keys %peers){
		next if ($peer eq '127.0.0.1' or $peer eq 'localhost');
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/result?qid=' . int($peers{$peer});
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		$results{$peer} = http_get $url, headers => $headers, sub {
			my ($body, $hdr) = @_;
			$self->log->debug('got results body from peer ' . $peer . ': ' . Dumper($body));
			try {
				my $raw_results = $self->json->decode($body);
				if ($raw_results and not $raw_results->{error} and not $raw_results->{incomplete}){
					my $num_results = $raw_results->{totalRecords} ? $raw_results->{totalRecords} : $raw_results->{recordsReturned};
					# Update any entries necessary
					$query = 'SELECT * FROM foreign_queries WHERE ISNULL(completed) AND qid=? AND peer=?';
					$sth = $self->db->prepare($query);
					$sth->execute($args->{qid}, $peer);
					if (my $row = $sth->fetchrow_hashref){
						$query = 'UPDATE foreign_queries SET completed=UNIX_TIMESTAMP() WHERE qid=? AND peer=?';
						$sth = $self->db->prepare($query);
						$sth->execute($args->{qid}, $peer);
						$self->log->trace('Set foreign_query ' . $args->{qid} . ' on peer ' . $peer . ' complete');
						
						if ($num_results){
							$query = 'UPDATE query_log SET num_results=num_results + ? WHERE qid=?';
							$sth = $self->db->prepare($query);
							$sth->execute($num_results, $args->{qid});
							$self->log->trace('Updated num_results for qid ' . $args->{qid} 
								. ' with ' . $num_results . ' additional records.');
						}
					}
					$results{$peer} = { %$raw_results }; #undef's the guard
				}
				else {
					$results{$peer} = 0;
				}
				$stats{$peer}->{total_request_time} = (time() - $start);
			}
			catch {
				my $e = catch_any(shift);
				$self->log->error($e->message . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $e->message, { peer => $peer });
				delete $results{$peer};
			};
			$cv->end;
		};
	}
	$cv->end;
}

sub local_result {
	my $cb = pop(@_);
	my ($self, $args) = @_;
	
	my $ret;
	
	try {
		unless ($args and ref($args) eq 'HASH' and $args->{qid}){
			$self->add_warning(500, 'Invalid args: ' . Dumper($args));
			$cb->();
			return;
		}
		
		# Authenticate the hash if given (so that the uid doesn't have to match)
		if ($args->{hash} and $args->{hash} ne $self->_get_hash($args->{qid})){
			$self->add_warning(401, q{You are not authorized to view another user's saved queries});
			$cb->();
			return;
		}
		
		my ($query, $sth);
		
		my @values = ($args->{qid});
		$query = 'SELECT t2.uid, t2.query, milliseconds FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
			'WHERE t1.qid=?';
		if ($args->{user} and not $args->{hash}){
			unless ($args->{user}){
				throw(401, 'Unauthorized');
			}
			$query .= ' AND uid=?';
			push @values, $args->{user}->uid;
		}
		$self->log->debug('query: ' . $query);
		
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my $row = $sth->fetchrow_hashref;
		unless ($row){
			$self->add_warning(404, 'No saved results for qid ' . $args->{qid} . ' found.');
			$cb->();
			return;
		}
		
		$query = 'SELECT data FROM saved_results_data WHERE qid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid});
		$row = $sth->fetchrow_hashref;
		
		my $data = $self->json->decode($row->{data});
		$self->log->debug('returning data: ' . Dumper($data));
		if (ref($data) and ref($data) eq 'ARRAY'){
			$cb->( { results => $data } );
		}
		else {
			$cb->($data);
		}
	}
	catch {
		my $e = shift;
		$self->log->error($e);
		$self->add_warning(500, $e);
		$cb->();
	};
}

sub _get_foreign_saved_result {
	my ($self, $args, $cb) = @_;
	
	# Switch over from this API to the Peers API for a global query using web services
	my $peer = '127.0.0.1';
	my $peer_conf = $self->conf->get('peers/' . $peer);
	unless ($peer_conf){
		$peer = 'localhost';
		$peer_conf = $self->conf->get('peers/' . $peer);
	}
	unless ($peer_conf){
		throw(500, 'No local $peer_conf for web services', { foreign_result => $peer });
	}
	my $url = $peer_conf->{url} . 'API/result?qid=' . int($args->{qid});
	$self->log->trace('Sending request to URL ' . $url);
	my $start = time();
	my $headers = { 
		Authorization => $self->_get_auth_header($peer),
	};
	# Do not proxy localhost requests
	my @no_proxy = ();
	if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
		push @no_proxy, proxy => undef;
	}
	my $cv = AnyEvent->condvar;
	my $ret;
	$cv->begin(sub { $cb->($ret); });
	my $guard; $guard = http_get $url, headers => $headers, @no_proxy, sub {
		my ($body, $hdr) = @_;
		eval {
			$ret = $self->json->decode($body);
			$self->log->trace('got foreign results: ' . Dumper($ret));
		};
		if ($@){
			$self->log->error($@ . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
			$self->add_warning(502, 'peer ' . $peer . ': ' . $@, { http => $peer });
		}
		$cv->end;
		undef $guard;
	};
}

sub get_permissions {
	my ($self, $args, $cb) = @_;
	
	throw(403, 'Admin required', { admin => 1 }) unless $args->{user}->is_admin;
		
	$self->get_form_params($args->{user}, sub {
		my $form_params = shift;
	
		# Build programs hash
		my $programs = {};
		foreach my $class_id (keys %{ $form_params->{programs} }){
			foreach my $program_name (keys %{ $form_params->{programs}->{$class_id} }){
				$programs->{ $form_params->{programs}->{$class_id}->{$program_name} } = $program_name;
			}
		}
		
		my ($query, $sth);
		
		$query = 'SELECT t3.uid, username, t1.gid, groupname, COUNT(DISTINCT(t4.attr_id)) AS has_exceptions' . "\n" .
			'FROM groups t1' . "\n" .
			'LEFT JOIN users_groups_map t2 ON (t1.gid=t2.gid)' . "\n" .
			'LEFT JOIN users t3 ON (t2.uid=t3.uid)' . "\n" .
			'LEFT JOIN permissions t4 ON (t1.gid=t4.gid)' . "\n" .
			'WHERE groupname LIKE CONCAT("%", ?, "%")' . "\n" . 
			'GROUP BY t1.gid' . "\n" .
			'ORDER BY t1.gid ASC';
		
		my @values;
		if ($args->{search}){
			push @values, $args->{search};
		}
		else {
			push @values, '';
		}
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my @ldap_entries;
		while (my $row = $sth->fetchrow_hashref){
			push @ldap_entries, $row;
		}
		
		$query = 'SELECT t2.groupname, t1.gid, attr, attr_id' . "\n" .
			'FROM permissions t1' . "\n" .
			'JOIN groups t2 ON (t1.gid=t2.gid)' . "\n" .
			'WHERE t1.gid=?';
		$sth = $self->db->prepare($query);
		foreach my $ldap_entry (@ldap_entries){
			$sth->execute($ldap_entry->{gid});
			my %exceptions;
			while (my $row = $sth->fetchrow_hashref){
				$self->log->debug('got row: ' . Dumper($row));
				if ($row->{attr}){
					$exceptions{ $row->{attr} } ||= {};
					if ($row->{attr} eq 'class_id'){
						$row->{attr_value} = $form_params->{classes_by_id}->{ $row->{attr_id} };
						if ($row->{attr_value}){
							$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
						}
					}
					elsif ($row->{attr} eq 'program_id'){
						$row->{attr_value} = $programs->{ $row->{attr_id} };
						if ($row->{attr_value}){
							$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
						}
					}
					elsif ($row->{attr} eq 'host_id'){
						# Must be host_id == IP or IP range
						if ($row->{attr_id} =~ /^\d+$/){
							$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
						}
						elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
							$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
						}
						else {
							$self->_error('bad host: ' . Dumper($args));
							return;
						}
						$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
					}
					elsif ($row->{attr} eq 'node_id'){
						# Must be node_id == IP or IP range
						if ($row->{attr_id} =~ /^\d+$/){
							$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
						}
						elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
							$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
						}
						else {
							$self->_error('bad host: ' . Dumper($args));
							return;
						}
						$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
					}
					else {
						$exceptions{ $row->{attr} }->{ $row->{attr_id} } = $row->{attr_id};
					}
					$self->log->debug('attr=' . $row->{attr} . ', attr_id=' . $row->{attr_id} . ', attr_value=' . $row->{attr_value});
				}
			}
			$ldap_entry->{_exceptions} = { %exceptions };
		}
		
		$query = 'SELECT gid, filter FROM filters WHERE gid=?';
		$sth = $self->db->prepare($query);
		my %filters;
		foreach my $ldap_entry (@ldap_entries){
			$sth->execute($ldap_entry->{gid});
			while (my $row = $sth->fetchrow_hashref){
				$ldap_entry->{_exceptions}->{filter}->{ $row->{filter} } = $row->{filter};
			}
		}
		
		my $permissions = {
			totalRecords => scalar @ldap_entries,
			records_returned => scalar @ldap_entries,
			results => [ @ldap_entries ],	
		};
		
		$permissions->{form_params} = $form_params;
		
		$cb->($permissions);
	});
}

sub set_permissions {
	my ($self, $args, $cb) = @_;
	
	throw(403, 'Admin required', { admin => 1 }) unless $args->{user}->is_admin;
	
	unless ($args->{action} and ($args->{action} eq 'add' or $args->{action} eq 'delete')){
		$self->_error('No set permissions action given: ' . Dumper($args));
		return;
	}
	try { 
		$args->{permissions} = $self->json->decode( $args->{permissions} ); 
	}
	catch {
		my $e = shift;
		$self->log->error('Error: ' . $e . ' args: ' . Dumper($args));
		throw(500, 'Error decoding permissions: ' . $e);
	};
	unless ( $args->{permissions} and ref( $args->{permissions} ) eq 'ARRAY' ) {
		throw(500, 'Invalid permissions args: ' . Dumper($args));
	}
	
	my ($query, $sth);
	my $rows_updated = 0;
	foreach my $perm (@{ $args->{permissions} }){
		my $short_attr = $perm->{attr};
		$short_attr =~ /([^\.]+)$/;
		$short_attr = $1;
		if ($Fields::IP_fields->{ $short_attr } and $perm->{attr_id} =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/){
			$perm->{attr_id} = unpack('N*', inet_aton($1)) . '-' . unpack('N*', inet_aton($2));
		}
		elsif ($Fields::IP_fields->{ $short_attr } and $perm->{attr_id} !~ /^[\d\-]+$/){
			$perm->{attr_id} = unpack('N*', inet_aton($perm->{attr_id}));
		}
		
		$self->log->info('Changing permissions: ' . join(', ', $args->{action}, $perm->{gid}, $perm->{attr}, $perm->{attr_id}));
		if ($args->{action} eq 'add'){
			if ($perm->{attr} eq 'filter'){
				$query = 'INSERT INTO filters (gid, filter) VALUES(?,?)';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr_id});
			}
			else {
				$query = 'INSERT INTO permissions (gid, attr, attr_id) VALUES (?,?,?)';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr}, $perm->{attr_id});
			}
		}
		elsif ($args->{action} eq 'delete'){
			if ($perm->{attr} eq 'filter'){
				$query = 'DELETE FROM filters WHERE gid=? AND filter=?';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr_id});
			}
			else {
				$query = 'DELETE FROM permissions WHERE gid=? AND attr=? AND attr_id=?';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr}, $perm->{attr_id});
			}
		}
		$rows_updated += $sth->rows;
	}
	$cb->( {success => $rows_updated, groups_changed => $rows_updated} );	
}

sub get_stats {
	my ($self, $args, $cb) = @_;
	my $user = $args->{user};
		
	my ($query, $sth);
	my $stats = {};
	my $days_ago = 7;
	if ($args->{seconds}){
		$days_ago = int($args->{seconds} / 86_400);
	}
	my $limit = 20;
	if ($args->{limit}){
		$limit = int($args->{limit});
	}
	
	# Queries per user
	$query = 'SELECT username, COUNT(*) AS count FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid)' . "\n" .
		'WHERE timestamp > DATE_SUB(NOW(), INTERVAL ? DAY)' . "\n" .
		'GROUP BY t1.uid ORDER BY count DESC LIMIT ?';
	$sth = $self->db->prepare($query);
	$sth->execute($days_ago, $limit);
	$stats->{queries_per_user} = { x => [], User => [] };
	while (my $row = $sth->fetchrow_hashref){
		push @{ $stats->{queries_per_user}->{x} }, $row->{username};
		push @{ $stats->{queries_per_user}->{User} }, $row->{count};
	}
	
	# General query stats
	$query = 'SELECT DATE_FORMAT(timestamp, "%Y-%m-%d") AS x, COUNT(*) AS Count, AVG(milliseconds) AS Avg_Time, ' . "\n" .
		'SUM(num_results) AS Results, AVG(num_results) AS Avg_Results' . "\n" .
		'FROM query_log WHERE timestamp > DATE_SUB(NOW(), INTERVAL ? DAY) GROUP BY x LIMIT ?';
	$sth = $self->db->prepare($query);
	$sth->execute($days_ago, $limit);
	$stats->{query_stats} = { x => [], Count => [], Avg_Time => [], Avg_Results => [] };
	while (my $row = $sth->fetchrow_hashref){
		foreach my $col (keys %{ $stats->{query_stats} }){
			push @{ $stats->{query_stats}->{$col} }, $row->{$col};
		}
	}
		
	my $intervals = 100;
	if ($args->{intervals}){
		$intervals = sprintf('%d', $args->{intervals});
	}
	
	
	# Get load stats
	my $load_stats = {};
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { $cb->($stats) });
	$self->_get_db(sub {
		my $db = shift;
		foreach my $item (qw(load archive index)){
			$load_stats->{$item} = {
				data => {
					x => [],
					LogsPerSec => [],
					KBytesPerSec => [],
				},
			};
									
			$query = 'SELECT MIN(bytes) AS min_bytes, AVG(bytes) AS avg_bytes, MAX(bytes) AS max_bytes,' . "\n" .
				'MIN(count) AS min_count, AVG(count) AS avg_count, MAX(count) AS max_count,' . "\n" .
				'UNIX_TIMESTAMP(MAX(timestamp))-UNIX_TIMESTAMP(MIN(timestamp)) AS total_time, UNIX_TIMESTAMP(MIN(timestamp)) AS earliest' . "\n" .
				'FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
			
			$cv->begin;
			unless ($db->{dbh}){
				throw(500, 'no dbh:' . Dumper($db));
			}
			$self->log->trace('get stat ' . $item);
			$db->{dbh}->query($query, $item, $args->{start}, $args->{end}, sub {
				my ($dbh, $rows, $rv) = @_;
				$self->log->trace('got stat ' . $item . ': ' . Dumper($rows));
				$load_stats->{$item}->{summary} = $rows->[0];
				$cv->end;
			
				$query = 'SELECT UNIX_TIMESTAMP(timestamp) AS ts, timestamp, bytes, count FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
				$cv->begin;
				$db->{dbh}->query($query, $item, $args->{start}, $args->{end}, sub {
					my ($dbh, $rows, $rv) = @_;
					unless ($intervals and $load_stats->{$item}->{summary} and $load_stats->{$item}->{summary}->{total_time}){
						$self->log->error('no stat for stat ' . $item);
						$cv->end;
						return;
					}
					$self->log->trace('$load_stats->{$item}->{summary}: ' . Dumper($load_stats->{$item}->{summary}));
					# arrange in the number of buckets requested
					my $bucket_size = ($load_stats->{$item}->{summary}->{total_time} / $intervals);
					unless ($bucket_size){
						$self->log->error('no bucket size, stat ' . $item);
						$cv->end;
						return;
					}
					foreach my $row (@$rows){
						my $ts = $row->{ts} - $load_stats->{$item}->{summary}->{earliest};
						my $bucket = int(($ts - ($ts % $bucket_size)) / $bucket_size);
						# Sanity check the bucket because too large an index array can cause an OoM error
						if ($bucket > $intervals){
							throw(500, 'Bucket ' . $bucket . ' with bucket_size ' . $bucket_size . ' and ts ' . $row->{ts} . ' was greater than intervals ' . $intervals, { stats => 1 });
						}
						unless ($load_stats->{$item}->{data}->{x}->[$bucket]){
							$load_stats->{$item}->{data}->{x}->[$bucket] = $row->{timestamp};
						}
						
						unless ($load_stats->{$item}->{data}->{LogsPerSec}->[$bucket]){
							$load_stats->{$item}->{data}->{LogsPerSec}->[$bucket] = 0;
						}
						$load_stats->{$item}->{data}->{LogsPerSec}->[$bucket] += ($row->{count} / $bucket_size);
						
						unless ($load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket]){
							$load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket] = 0;
						}
						$load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket] += ($row->{bytes} / 1024 / $bucket_size);
					}
					$cv->end;
				});
			});
		}
			
		# Combine the stats info for the nodes
		my $combined = {};
		#$self->log->debug('got stats: ' . Dumper($stats->{nodes}));
		
		foreach my $stat (qw(load index archive)){
			$combined->{$stat} = { x => [], LogsPerSec => [], KBytesPerSec => [] };
			foreach my $node (keys %{ $stats->{nodes} }){
				if ($stats->{nodes}->{$node} and $stats->{nodes}->{$node}->{$stat}){ 
					my $load_data = $stats->{nodes}->{$node}->{$stat}->{data};
					next unless $load_data;
					for (my $i = 0; $i < (scalar @{ $load_data->{x} }); $i++){
						next unless $load_data->{x}->[$i];
						unless ($combined->{$stat}->{x}->[$i]){
							$combined->{$stat}->{x}->[$i] = $load_data->{x}->[$i];
						}
						$combined->{$stat}->{LogsPerSec}->[$i] += $load_data->{LogsPerSec}->[$i];
						$combined->{$stat}->{KBytesPerSec}->[$i] += $load_data->{KBytesPerSec}->[$i];
					}
				}	
			}
		}
		$stats->{combined_load_stats} = $combined;
		
		$self->_get_info(1, sub {
			my $info = shift;
			$stats->{start} = $info->{indexes_min} ? epoch2iso($info->{indexes_min}) : epoch2iso($info->{archive_min});
			$stats->{start_int} = $info->{indexes_min} ? $info->{indexes_min} : $info->{archive_min};
			$stats->{display_start_int} = $info->{indexes_min} ? $info->{indexes_min} : $info->{archive_min};
			$stats->{archive_start} = epoch2iso($info->{archive_min});
			$stats->{archive_start_int} = $info->{archive_min};
			$stats->{archive_display_start_int} = $info->{archive_min};
			$stats->{end} = $info->{indexes_max} ? epoch2iso($info->{indexes_max}) : epoch2iso($info->{archive_max});
			$stats->{end_int} = $info->{indexes_max} ? $info->{indexes_max} : $info->{archive_max};
			$stats->{archive_end} = epoch2iso($info->{archive_max});
			$stats->{archive_end_int} = $info->{archive_max};
			$stats->{totals} = $info->{totals};
				
			$self->log->debug('got stats: ' . Dumper($stats));
			$cv->end;
		});
	});
}

sub _merge_node_info {
	my ($self, $results) = @_;
	#$self->log->debug('merging: ' . Dumper($results));
	
	# Merge these results
	my $overall_final = merge values %$results;
	
	# Merge the times and counts
	my %final = (nodes => {});
	foreach my $peer (keys %$results){
		next unless $results->{$peer} and ref($results->{$peer}) eq 'HASH';
		if ($results->{$peer}->{nodes}){
			foreach my $node (keys %{ $results->{$peer}->{nodes} }){
				if ($node eq '127.0.0.1' or $node eq 'localhost'){
					$final{nodes}->{$peer} ||= $results->{$peer}->{nodes};
				}
				else {
					$final{nodes}->{$node} ||= $results->{$peer}->{nodes};
				}
			}
		}
		foreach my $key (qw(archive_min indexes_min)){
			if (not $final{$key} or $results->{$peer}->{$key} < $final{$key}){
				$final{$key} = $results->{$peer}->{$key};
			}
		}
		foreach my $key (qw(archive indexes)){
			$final{totals} ||= {};
			$final{totals}->{$key} += $results->{$peer}->{totals}->{$key};
		}
		foreach my $key (qw(archive_max indexes_max indexes_start_max archive_start_max)){
			if (not $final{$key} or $results->{$peer}->{$key} > $final{$key}){
				$final{$key} = $results->{$peer}->{$key};
			}
		}
	}
	$self->log->debug('final: ' . Dumper(\%final));
	foreach my $key (keys %final){
		$overall_final->{$key} = $final{$key};
	}
	
	return $overall_final;
}

sub get_form_params {
	my ( $self, $user, $cb) = @_;
	
	$user ||= new User(username => 'system');
	my $args = { user => $user };
	
	my $form_params;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer;
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my %results;
	my %stats;
	
	try {
		my $cv = AnyEvent->condvar;
		$cv->begin(sub { 
			my $overall_final = $self->_merge_node_info(\%results);
			$stats{overall} = (time() - $overall_start);
			$self->log->debug('stats: ' . Dumper(\%stats));
			$self->meta_info($overall_final);
			my $default_rows_per_page = defined $self->conf->get('default_rows_per_page') ? $self->conf->get('default_rows_per_page') : 15;
			#if ($user->preferences->{tree}->{default_settings}->{rows_per_page}){
			if ($user->preferences and 
				$user->preferences->{tree} and
				$user->preferences->{tree}->{default_settings} and
				$user->preferences->{tree}->{default_settings}->{rows_per_page}){
				$default_rows_per_page = $user->preferences->{tree}->{default_settings}->{rows_per_page};
			}
				
			$form_params = {
				start => $self->meta_info->{indexes_min} ? epoch2iso($self->meta_info->{indexes_min}) : epoch2iso($self->meta_info->{archive_min}),
				start_int => $self->meta_info->{indexes_min} ? $self->meta_info->{indexes_min} : $self->meta_info->{archive_min},
				display_start_int => $self->meta_info->{indexes_min} ? $self->meta_info->{indexes_min} : $self->meta_info->{archive_min},
				archive_start => epoch2iso($self->meta_info->{archive_min}),
				archive_start_int => $self->meta_info->{archive_min},
				archive_display_start_int => $self->meta_info->{archive_min},
				end => $self->meta_info->{indexes_max} ? epoch2iso($self->meta_info->{indexes_max}) : epoch2iso($self->meta_info->{archive_max}),
				end_int => $self->meta_info->{indexes_max} ? $self->meta_info->{indexes_max} : $self->meta_info->{archive_max},
				archive_end => epoch2iso($self->meta_info->{archive_max}),
				archive_end_int => $self->meta_info->{archive_max},
				classes => $self->meta_info->{classes},
				classes_by_id => $self->meta_info->{classes_by_id},
				fields => $self->meta_info->{fields},
				nodes => [ keys %{ $self->conf->get('peers') } ],
				groups => $user->groups,
				additional_display_columns => $self->conf->get('additional_display_columns') ? $self->conf->get('additional_display_columns') : [],
				totals => $self->meta_info->{totals},
				preferences => $user->preferences,
				version => $self->meta_info->{version},
				rows_per_page => $default_rows_per_page, 
			};
			# You can change the default start time displayed to web users by changing this config setting
			if ($self->conf->get('default_start_time_offset')){
				$form_params->{display_start_int} = ($form_params->{end_int} - (86400 * $self->conf->get('default_start_time_offset')));
			}
			
			if ($user->username ne 'system'){
				# this is for a user, restrict what gets sent back
				unless ($user->permissions->{class_id}->{0}){
					foreach my $class_id (keys %{ $form_params->{classes} }){
						unless ($user->permissions->{class_id}->{$class_id}){
							delete $form_params->{classes}->{$class_id};
						}
					}
				
					my $possible_fields = [ @{ $form_params->{fields} } ];
					$form_params->{fields} = [];
					for (my $i = 0; $i < scalar @$possible_fields; $i++){
						my $field_hash = $possible_fields->[$i];
						my $class_id = $field_hash->{class_id};
						if ($user->permissions->{class_id}->{$class_id}){
							push @{ $form_params->{fields} }, $field_hash;
						}
					}
				}
			}
			
			# Tack on the "ALL" and "NONE" special types
			unshift @{$form_params->{fields}}, 
				{'value' => 'ALL', 'text' => 'ALL', 'class_id' => 0 }, 
				{'value' => 'NONE', 'text' => 'NONE', 'class_id' => 1 };
			
			$form_params->{schedule_actions} = $self->_get_schedule_actions($user);	
			$cb->($form_params);
		});
		
		foreach my $peer (@peers){
			$cv->begin;
			my $peer_conf = $self->conf->get('peers/' . $peer);
			my $url = $peer_conf->{url} . 'API/';
			$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_info' : 'info';
			$self->log->trace('Sending request to URL ' . $url);
			my $start = time();
			my $headers = { 
				Authorization => $self->_get_auth_header($peer),
			};
			# Do not proxy localhost requests
			my @no_proxy = ();
			if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
				push @no_proxy, proxy => undef;
			}
			$results{$peer} = http_get $url, headers => $headers, @no_proxy, sub {
				my ($body, $hdr) = @_;
				eval {
					my $raw_results = $self->json->decode($body);
					$stats{$peer} ||= {};
					$stats{$peer}->{total_request_time} = (time() - $start);
					$results{$peer} = { %$raw_results }; #undef's the guard
				};
				if ($@){
					$self->log->error($@ . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
					$self->add_warning(502, 'peer ' . $peer . ': ' . $@, { http => $peer });
					delete $results{$peer};
				}
				$cv->end;
			};
		}
		$cv->end;
	}
	catch {
		my $e = shift;
		$self->log->error($e);
		throw(500, 'Error getting form params');
	};
}

sub _get_schedule_actions {
	my ($self, $user) = @_;
	
	my @ret;
	foreach my $plugin ($self->connector_plugins()){
		if ($plugin =~ /^Connector::(\w+)/){
			unless ($user->is_admin){
				next if $plugin->admin_required;
			}
			my $desc = $plugin->description;
			$self->log->debug('plugin: ' . $plugin . ', desc: ' . "$desc");
			push @ret, { action => $1 . '()', description => $desc };
		}
	}
	return \@ret;
}

sub get_scheduled_queries {
	my ($self, $args, $cb) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	my $orderby = 'id';
	if ($args->{sort} and $Scheduled_query_cols->{ $args->{sort} }){
		$orderby = $args->{sort};
	}
	my $dir = 'DESC';
	if ($args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*) AS totalRecords FROM query_schedule' . "\n" .
		'WHERE uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid);
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};

	$query = 'SELECT t1.id, query, frequency, start, end, connector, params, enabled, UNIX_TIMESTAMP(last_alert) As last_alert, alert_threshold' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'WHERE uid=?' . "\n" .
		'ORDER BY ' . $orderby . ' ' . $dir . "\n" .
		'LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid, $offset, $limit);
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		push @rows, $row;
	}
	my $ret = {
		'results' => [ @rows ],
		'totalRecords' => $totalRecords,
		'recordsReturned' => scalar @rows,
	};
	$cb->($ret);
}

sub get_all_scheduled_queries {
	my ($self, $args, $cb) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	throw(403, 'Admin required', { admin => 1 }) unless $args->{user}->is_admin;
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	my $orderby = 'id';
	if ($args->{sort} and $Scheduled_query_cols->{ $args->{sort} }){
		$orderby = $args->{sort};
	}
	my $dir = 'DESC';
	if ($args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*) AS totalRecords FROM query_schedule';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};

	$query = 'SELECT t1.id, username, query, frequency, start, end, connector, params, enabled, UNIX_TIMESTAMP(last_alert) As last_alert, alert_threshold' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users t2 ON (t1.uid=t2.uid)' . "\n" .
		'ORDER BY ' . $orderby . ' ' . $dir . "\n" .
		'LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($offset, $limit);
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		push @rows, $row;
	}
	my $ret = {
		'results' => [ @rows ],
		'totalRecords' => $totalRecords,
		'recordsReturned' => scalar @rows,
	};
	$cb->($ret);
}

sub set_preference {
	my ($self, $args, $cb) = @_;
	
	foreach my $item (qw(id type name value)){	
		unless (defined $args->{$item}){
			$self->_error('Invalid args, missing arg: ' . $item);
			return;
		}
	}
	
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	$self->log->info('Updating preferences: ' . Dumper(($args->{type}, $args->{name}, $args->{value}, $args->{id}, $args->{uid})));
	
	my ($query, $sth);
	$query = 'UPDATE preferences SET type=?, name=?, value=? WHERE id=? AND uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{type}, $args->{name}, $args->{value}, $args->{id}, $args->{uid});
	
	$query = 'SELECT * FROM preferences WHERE id=? AND uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{id}, $args->{uid});
	my $row = $sth->fetchrow_hashref;
        $row->{name} = unicode_escape($row->{name});
        $row->{type} = unicode_escape($row->{type});
        $row->{value} = unicode_escape($row->{value});
	$cb->($row);
}

sub add_preference {
	my ($self, $args, $cb) = @_;
	
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	$self->log->info('Adding new empty preference');
	
	my ($query, $sth);
	$query = 'INSERT INTO preferences (uid, type) VALUES(?, "custom")';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{uid});
	
	$query = 'SELECT * FROM preferences WHERE uid=? ORDER BY id DESC LIMIT 1';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{uid});
	
	$cb->($sth->fetchrow_hashref);
}

sub delete_preference {
	my ($self, $args, $cb) = @_;
	
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	$self->log->info('Deleting preferences: ' . Dumper(($args->{type}, $args->{name}, $args->{value}, $args->{id}, $args->{uid})));
	
	my ($query, $sth);
	$query = 'DELETE FROM preferences WHERE uid=? AND id=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{uid}, $args->{id});
        $args->{id} = unicode_escape($args->{id});
	
	$cb->({ id => $args->{id} });
}

sub schedule_query {
	my ($self, $args, $cb) = @_;
	
	foreach my $item (qw(qid days time_unit)){	
		unless (defined $args->{$item}){
			$self->_error('Invalid args, missing arg: ' . $item);
			return;
		}
	}
	
	# Make sure these params are ints
	foreach my $item (qw(qid days time_unit count)){
		next unless $args->{$item};
		$args->{$item} = sprintf('%d', $args->{$item});
	}
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	my %standard_vars = map { $_ => 1 } (qw(uid qid days time_unit count connector connector_params));
	my $schedule_query_params = { params => {} };
	foreach my $item (keys %{$args}){
		if ($standard_vars{$item}){
			$schedule_query_params->{$item} = $args->{$item};
		}
		else {
			$schedule_query_params->{params}->{$item} = $args->{$item};
		}
	}
	$schedule_query_params->{params} = $self->json->encode($schedule_query_params->{params});
	
	# Add on the connector params and sanitize
	my @connector_params = split(/,/, $schedule_query_params->{connector_params});
	foreach (@connector_params){
		$_ =~ s/[^a-zA-Z0-9\.\_\-\ ]//g;
	}
	if ($schedule_query_params->{connector} =~ s/\(([^\)]*)\)$//){
		unshift @connector_params, split(/,/, $1);
	}
	$schedule_query_params->{connector} .= '(' . join(',', @connector_params) . ')';
		
	my @frequency;
	for (my $i = 1; $i <= 7; $i++){
		if ($i eq $schedule_query_params->{time_unit}){
			$frequency[$i-1] = 1;
		}
		else {
			$frequency[$i-1] = 0;
		}
	}
	my $freq_str = join(':', @frequency);
	$self->log->debug('freq_str: ' . $freq_str);
	
	my ($query, $sth);
	$query = 'INSERT INTO query_schedule (uid, query, frequency, start, end, connector, params, last_alert, alert_threshold) VALUES (?, ' . "\n" .
		'(SELECT query FROM query_log WHERE qid=?), ?, ?, ?, ?, ?, "1970-01-01 00:00:00", ?)';
	$sth = $self->db->prepare($query);
	my $days = $schedule_query_params->{days};
	unless ($days){
		$days = 2**32;
	}
	my $alert_threshold = 0;
	my $time_unit_map = {
		1 => (60 * 60 * 24 * 365),
		2 => (60 * 60 * 24 * 30),
		3 => (60 * 60 * 24 * 7),
		4 => (60 * 60 * 24),
		5 => (60 * 60),
		6 => (60),
	};
	if ($schedule_query_params->{count} and $schedule_query_params->{time_unit}){
		$alert_threshold = $time_unit_map->{ $schedule_query_params->{time_unit} } * $schedule_query_params->{count};
	}
	$sth->execute($schedule_query_params->{uid}, $schedule_query_params->{qid}, $freq_str, time(), (86400 * $days) + time(), 
		$schedule_query_params->{connector}, $schedule_query_params->{params}, $alert_threshold);
	my $ok = $sth->rows;
	
	$cb->($ok);
}

sub delete_saved_results {
	my ($self, $args, $cb) = @_;
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{qid}){
		throw(400, 'Invalid args, no qid');
	}
	my ($query, $sth);
	# Verify this query belongs to the user
	$query = 'SELECT uid FROM query_log WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		throw(400, 'Invalid args, no results found for qid');
	}
	unless ($row->{uid} eq $args->{user}->uid or $args->{user}->is_admin){
		$self->_error('Unable to alter these saved results based on your authorization: ' . Dumper($args));
		return;
	}
	$query = 'DELETE FROM saved_results WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	if ($sth->rows){
		$cb->({deleted => $sth->rows});
	}
	else {
		throw(404, 'Query ID not found!');
	}
}

sub delete_scheduled_query {
	my ($self, $args, $cb) = @_;
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{id}){
		$self->_error('Invalid args, no id: ' . Dumper($args));
		return;
	}
	my ($query, $sth);
	$query = 'DELETE FROM query_schedule WHERE uid=? AND id=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid, $args->{id});
	if ($sth->rows){
		$cb->({deleted => $sth->rows});
	}
	else {
		throw(404, 'Schedule ID ' . $args->{id} . ' not found!');
	}
}

sub update_scheduled_query {
	my ($self, $args, $cb) = @_;
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{id}){
		throw(400, 'Invalid args, no id: ' . Dumper($args));
	}
	my $attr_map = {};
	foreach my $item (qw(query frequency start end connector params enabled alert_threshold)){
		$attr_map->{$item} = $item;
	}
	my ($query, $sth);
	my $new_args = {};
	foreach my $given_arg (keys %{ $args }){
		next if $given_arg eq 'id' or $given_arg eq 'user';
		unless ($attr_map->{$given_arg}){
			throw(400, 'Invalid arg: ' . $given_arg);
			return;
		}
		
		# Decode
		$args->{$given_arg} = uri_unescape($args->{$given_arg});
		
		# Chop quotes
		$args->{$given_arg} =~ s/^['"](.+)['"]$/$1/;
		
		# Adjust timestamps if necessary
		if ($given_arg eq 'start' or $given_arg eq 'end'){
			$args->{$given_arg} = UnixDate($args->{$given_arg}, '%s');
		}
		
		$self->log->debug('given_arg: ' . $given_arg . ': ' . $args->{$given_arg});
		$query = sprintf('UPDATE query_schedule SET %s=? WHERE id=? AND uid=?', $given_arg);
		$sth = $self->db->prepare($query);
		$sth->execute($args->{$given_arg}, $args->{id}, $args->{user}->uid);
		$new_args->{$given_arg} = $args->{$given_arg};
	}
	
	$cb->($new_args);
}

sub save_results {
	my ($self, $args, $cb) = @_;
	$self->log->debug(Dumper($args));
	
	if (defined $args->{qid}){ # came from another Perl program, not the web, so no need to de-JSON
		$cb->($self->_save_results($args));
		return;
	}
	
	my $user = $args->{user};
	eval {
		my $comments = $args->{comments};
		my $num_results = $args->{num_results} if $args->{num_results};
		# Replace args so we wipe user, etc.
		$args = $self->json->decode($args->{results});
		$args->{comments} = $comments;
		$args->{num_results} = $num_results if $num_results;
	};
	if ($@){
		throw(500, $@);
		return;
	}
	unless ($args->{qid} and $args->{results} and ref($args->{results})){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	
	$self->log->debug('got results to save: ' . Dumper($args));
	$args->{user} = $user;
	
	$cb->($self->_save_results($args));
}

sub _save_results {
	my ($self, $args) = @_;
	
	unless ($args->{qid}){
		$self->log->error('No qid found');
		return;
	}
	
	my ($query, $sth);
	$query = 'SELECT uid FROM query_log WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	my $row = $sth->fetchrow_hashref;
	unless (not $args->{user} or ($row and $row->{uid} eq $args->{user}->uid)){
		throw(403, 'Insufficient permissions', { user => 1 });
	}
	
	$self->db->begin_work;
	$query = 'INSERT INTO saved_results (qid, comments) VALUES(?,?)';
	$sth = $self->db->prepare($query);
	
	$sth->execute($args->{qid}, $args->{comments});
	$query = 'INSERT INTO saved_results_data (qid, data) VALUES (?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid}, $self->json->encode($args));
	
	if ($args->{num_results}){
		$query = 'UPDATE query_log SET num_results=? WHERE qid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{num_results}, $args->{qid});
	}
		
	$self->db->commit;
	
	$self->log->info('Saved results to qid ' . $args->{qid});
	
	return 1;
}

sub get_saved_searches {
	my ($self, $args, $cb) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	
	my $uid = $args->{user}->uid;
	if ($args->{uid}){
		$uid = sprintf('%d', $args->{uid});
	}
	if ($uid ne $args->{user}->uid and not $args->{user}->is_admin){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;	
	}
	
	my ($query, $sth);
	$query = 'SELECT COUNT(*) AS totalRecords FROM preferences WHERE uid=? AND type="saved_query"';
	$sth = $self->db->prepare($query);
	$sth->execute($uid);
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};
	$query = 'SELECT * FROM preferences WHERE uid=? AND type="saved_query" ORDER BY id DESC LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($uid, $offset, $limit);
	my $saved_queries = [];
	while (my $row = $sth->fetchrow_hashref){
		push @$saved_queries, $row;
	}
	$self->log->debug( "saved_queries: " . Dumper($saved_queries) );
	$cb->({ 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$saved_queries,
		results => $saved_queries
	});
}

sub get_saved_results {
	my ($self, $args, $cb) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	
	my $uid = $args->{user}->uid;
	if ($args->{uid}){
		$uid = sprintf('%d', $args->{uid});
	}
	if ($uid ne $args->{user}->uid and not $args->{user}->is_admin){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;	
	}
	
	
	my $saved_results;
	if ($args->{qid} and not ($args->{startIndex} or $args->{results})){
		# We're just getting one known query
		$saved_results = $self->_get_saved_result(sprintf('%d', $args->{qid}));
	}
	else {
		$saved_results = $self->_get_saved_results($uid, $offset, $limit, $args->{search});
	}
	

	$self->log->debug( "saved_results: " . Dumper($saved_results) );
	$cb->($saved_results);
}

sub _get_saved_result {
	my ($self, $qid) = @_;
	
	my ( $query, $sth, $row );
	
	$query = 'SELECT t1.qid, t2.query, comments' . "\n" .
			'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
			'WHERE t2.qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($qid);
	
	return $sth->fetchrow_hashref or {error => 'QID ' . $qid . ' not found.'};
}

sub _get_saved_results {
	my ( $self, $uid, $offset, $limit, $search ) = @_;
	$limit = 100 unless $limit;

	my ( $query, $sth, $row );
	
	# First find total number
	$query =
	    'SELECT COUNT(*) AS totalRecords ' . "\n"
	  #. 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n"
	  . 'FROM saved_results t1 '
	  . 'LEFT JOIN foreign_queries t2 ON (t1.qid=t2.foreign_qid) '
	  . 'JOIN query_log t3 ON (t1.qid=t3.qid OR t2.qid=t3.qid) '
	  . 'WHERE uid=?'; #AND comments!=\'_alert\'';
	$sth = $self->db->prepare($query) or throw(500, $self->db->errstr, { mysql => 1 } );
	$sth->execute( $uid );
	$row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords} ? $row->{totalRecords} : 0;

	my @placeholders = ($uid);
	$query =
	    'SELECT t3.qid, t3.query, comments, num_results, UNIX_TIMESTAMP(timestamp) AS timestamp ' . "\n"
	  #. 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n"
	  . 'FROM saved_results t1 '
	  . 'LEFT JOIN foreign_queries t2 ON (t1.qid=t2.foreign_qid) '
	  . 'JOIN query_log t3 ON (t1.qid=t3.qid OR t2.qid=t3.qid) '
	  . 'WHERE uid=?' . "\n";
	if ($search){
		$query .= ' AND SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(t3.query, " ", IF(ISNULL(comments), "", comments)), \'"\', 4), \'"\', -1) LIKE CONCAT("%", ?, "%")' . "\n";
		push @placeholders, $search;
	}
	push @placeholders, $offset, $limit;
	$query .= 'ORDER BY qid DESC LIMIT ?,?';
	$self->log->debug(Dumper(\@placeholders));
	$sth = $self->db->prepare($query) or throw(500, $self->db->errstr, { mysql => 1 } );
	
	$sth->execute( @placeholders );


	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		# we have to decode this to make sure it doesn't end up as a string
		my $decode;
		eval { 
			$decode = $self->json->decode($row->{query}); 
		};
		
		my $query = $decode->{query_string};
		push @{$queries}, {
			qid => $row->{qid},
			timestamp => $row->{timestamp},
			query => $query, 
			num_results => $row->{num_results}, 
			comments => $row->{comments},
			hash => $self->_get_hash($row->{qid}),
		};
	}
	return { 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$queries,
		results => [ @{$queries} ] 
	};
}

sub get_previous_queries {
	my ($self, $args, $cb) = @_;
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = $self->conf->get('previous_queries_limit');
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	my $dir = 'DESC';
	if ( $args->{dir} and $args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	my $uid = $args->{user}->uid;
	
	my ( $query, $sth, $row );
	
	# First find total number
	$query =
	    'SELECT COUNT(*) AS totalRecords ' . "\n"
	  . 'FROM query_log ' . "\n"
	  . 'WHERE uid=?';
	$sth = $self->db->prepare($query) or throw(500, $self->db->errstr, { mysql => 1 } );
	$sth->execute( $uid );
	$row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords} ? $row->{totalRecords} : 0;

	# Find our type of database and use the appropriate query
	my $db_type = $self->db->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		# In MS-SQL, we don't have the niceties of OFFSET, so we have to do this via subqueries
		my $outer_top = $offset + $limit;
		$query = 'SELECT * FROM ' . "\n" .
			'(SELECT TOP ? qid, query, timestamp, num_results, milliseconds FROM ' . "\n" .
			'(SELECT TOP ? qid, query, timestamp, num_results, milliseconds FROM ' . "\n" .
			'FROM query_log ' . "\n" .
		  	'WHERE uid=?' . "\n" .
		  	'ORDER BY qid DESC) OverallTop ' . "\n" .
		  	'ORDER BY qid ASC) TopOfTop ' . "\n" .
		  	'ORDER BY qid DESC';
		$sth = $self->db->prepare($query) or throw(500, $self->db->errstr, { mysql => 1 } );
		$sth->execute($limit, ($offset + $limit), $uid);
	}
	else {
		$query =
		    'SELECT qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND system=0' . "\n"
		  . 'ORDER BY qid ' . $dir . ' LIMIT ?,?';
		$sth = $self->db->prepare($query) or throw(500, $self->db->errstr, { mysql => 1 } );
		$sth->execute( $uid, $offset, $limit );
	}

	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		if ( $row->{query} ) {

			# we have to decode this to make sure it doesn't end up as a string
			my $prev_query = $self->json->decode( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_string} )
			{
				push @{$queries},
				  {
					qid          => $row->{qid},
					query        => $prev_query->{query_string},
					query_obj    => $prev_query,
					timestamp    => $row->{timestamp},
					num_results  => $row->{num_results},
					milliseconds => $row->{milliseconds},
				  };

			}
		}
	}
	$cb->({ 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$queries,
		results => [ @{$queries} ] 
	});
}

sub get_running_archive_query {
	my ($self, $args, $cb) = @_;
	
	my ($query, $sth);
	$query = 'SELECT qid, query FROM query_log WHERE uid=? AND archive=1 AND num_results=-1';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid);
	my $row = $sth->fetchrow_hashref;
	if ($row){
		my $query_params = $self->json->decode($row->{query});
		 $cb->({ qid => $row->{qid}, query => $query_params->{query_string} });
	}
	else {
		 $cb->({qid => 0});
	}
}

sub query {
	my ($self, $args, $cb) = @_;
	
	my $ret;
	try {
		QueryParser->new(conf => $self->conf, log => $self->log, %$args, on_connect => sub {
			my $qp = shift;
			my $q = $qp->parse();
			$ret = $q;
			
			$self->_peer_query($q, sub {
				
				if (not $q->batch){
					$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
				
					$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000));
				}
				
				$q->dedupe_warnings();
				
				# Apply offset
				if ($q->offset and not $args->{peer_label}){ # do not apply offset if we were called recursively, only parent should offset
					my $counter = 0;
					foreach my $record ($q->results->all_results){
						$counter++;
						$q->results->delete_record($record);
						if ($counter >= $q->offset){
							last;
						}
					}
				}
				
				if ($q->has_connectors){
					$self->send_to($q, sub { 
						$cb->($q);
					});
				}
				else {
					$cb->($q);
				}
			});
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
#		if ($ret and ref($ret) and $ret->can('qid') and $ret->qid){
#			$ret->add_warning($e);
#			$cb->($ret);
#		}
#		else {
#			# return dummy
#			$ret = {
#				qid => 1,
#				totalTime => 0,
#				results => [], 
#				orderby_dir => 'DESC',
#				totalRecords => 0,
#				recordsReturned => 0,	
#				query_string => '',
#				query_meta_params => {},
#				highlights => {},
#				stats => {},
#				approximate => 0,
#				percentage_complete => 0,
#				errors => [ $e->message ],
#			};
#			$cb->($ret);
#			
#		}
	};
}

sub local_query {
	my $cb = pop(@_);
	my ($self, $args, $from_cron) = @_;
	
	# We may need to recursively redirect from one class to another, so the execution is wrapped in this sub
	my $run_query;
	$run_query = sub {
		my $qp = shift;
		my $cb = pop(@_);
		my $class = shift;
		my $extra_directives = shift;
		my $q = $qp->parse($class, $args->{qid});
		
		Log::Log4perl::MDC->put('qid', $q->qid);
		
		try {
			if ($self->conf->get('disallow_sql_search') and $qp->query_class eq 'Query::SQL'){
				my $msg;
				if (scalar keys %{ $qp->stopword_terms }){
					throw(413, 'Cannot execute query, terms too common: ' . join(', ', keys %{ $qp->stopword_terms }), { terms => join(', ', keys %{ $qp->stopword_terms }) });
				}
				else {
					throw(405, 'Query required SQL search which is not enabled', { search_type => 'SQL' });
				}
			}
			if ($extra_directives){
				# These directives were added by another class, not the user
				$self->log->trace('Extra directives: ' . Dumper($extra_directives));
				foreach my $directive (keys %$extra_directives){
					$q->$directive($extra_directives->{$directive});
				}
			}
			
			if ($from_cron){
				$q->mark_batch_start();
				$q->timeout(0);
				$q->execute(sub { $cb->($q) });
			}
			else {
				my $estimate_hash = $q->estimate_query_time();
				$self->log->trace('Query estimate: ' . Dumper($estimate_hash));
				
				if ($self->conf->get('query_time_batch_threshold')){
					$Query_time_batch_threshold = $self->conf->get('query_time_batch_threshold');
				}
				
				if ($estimate_hash->{estimated_time} > $Query_time_batch_threshold){
					$q->batch_message('Batching because estimated query time is ' . int($estimate_hash->{estimated_time}) . ' seconds.');
					$self->log->info($q->batch_message);
					$q->batch(1);
					$q->execute_batch(sub { $cb->($q) });
				}
				else {
					$q->execute(sub { $cb->($q) });
				}
			}
		}
		catch {
			my $e = shift;
			if (caught(302, $e)){
				my $redirected_class = $e->data->{location};
				my $directives = $e->data->{directives};
				throw(500, 'Redirected, but no class given', { term => $class }) unless $redirected_class;
				if ($class and $redirected_class and $redirected_class eq $class){
					throw(500, 'Class ' . $class . ' was the same as ' . $redirected_class . ', infinite loop.', { term => $class });
				}
				$self->log->info("Redirecting to $redirected_class");
				$run_query->($qp, $redirected_class, $directives, $cb);
			}
			else {
				die($e);
			}
		};
	};
	
	try {
		QueryParser->new(conf => $self->conf, log => $self->log, %$args, on_connect => sub {
			my $qp = shift;
			$run_query->($qp, sub {
				my $q = shift;
				foreach my $warning ($self->all_warnings){
					push @{ $q->warnings }, $warning;
				}
				
				if (not $q->batch){
					$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
				
					$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000));
			
					# Apply transforms
					$q->transform_results(sub { 
						$q->dedupe_warnings();
						$cb->($q);
					});
				}
				else {
					$self->log->info("Query " . $q->qid . " batched");
					$cb->($q);
				}
			});
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub local_query_preparsed {
	my ($self, $q, $cb) = @_;
	
	# We may need to recursively redirect from one class to another, so the execution is wrapped in this sub
	my $run_query;
	$run_query = sub {
		my $q = shift;
		my $cb = pop(@_);
		my $class = shift;
		my $extra_directives = shift;
		if ($class){
			$self->log->trace('Converting ' . $q->qid . ' from class ' . ref($q) . ' to ' . $class);
			$q = $q->parser->parse($class, $q->qid);
		}
		
		# Check connectors and transforms to be sure this will work
		if ($q->has_connectors){
			foreach my $connector ($q->all_connectors){
				my $found = 0;
				foreach my $plugin ($self->connector_plugins()){
					if ($plugin =~ /\:\:$connector(?:\:\:|$)/i){
						$found = 1;
						last;
					}
				}
				throw(400, 'Connector ' . $connector . ' not found', { connector => $connector}) unless $found;
			}
		}
		
		Log::Log4perl::MDC->put('qid', $q->qid);
		
		try {
			if ($self->conf->get('disallow_sql_search') and $q->parser->query_class eq 'Query::SQL'){
				my $msg;
				if (scalar keys %{ $q->parser->stopword_terms }){
					throw(413, 'Cannot execute query, terms too common: ' . join(', ', keys %{ $q->parser->stopword_terms }), { terms => join(', ', keys %{ $q->parser->stopword_terms }) });
				}
				else {
					throw(405, 'Query required SQL search which is not enabled', { search_type => 'SQL' });
				}
			}
			if ($extra_directives){
				# These directives were added by another class, not the user
				$self->log->trace('Extra directives: ' . Dumper($extra_directives));
				foreach my $directive (keys %$extra_directives){
					$q->$directive($extra_directives->{$directive});
				}
			}
			my $estimate_hash = $q->estimate_query_time();
			$self->log->trace('Query estimate: ' . Dumper($estimate_hash));
			#
			
			if ($self->conf->get('query_time_batch_threshold')){
				$Query_time_batch_threshold = $self->conf->get('query_time_batch_threshold');
			}
			
			if ($estimate_hash->{estimated_time} > $Query_time_batch_threshold){
				if ($self->conf->get('disallow_sql_search')){
					throw(405, 'Query required SQL search which is not enabled', { search_type => 'SQL' });
				}
				if ($q->meta_params->{nobatch}){
					$q->execute(sub { $cb->($q) });
				}
				else {
					$q->batch_message('Batching because estimated query time is ' . int($estimate_hash->{estimated_time}) . ' seconds.');
					$self->log->info($q->batch_message);
					$q->batch(1);
					$q->execute_batch(sub { $cb->($q) });
				}
			}
			else {
				$q->execute(sub { $cb->($q) });
			}
		}
		catch {
			my $e = shift;
			if (caught(302, $e)){
				my $redirected_class = $e->data->{location};
				my $directives = $e->data->{directives};
				throw(500, 'Redirected, but no class given', { term => $class }) unless $redirected_class;
				if ($class and $redirected_class and $redirected_class eq $class){
					throw(500, 'Class ' . $class . ' was the same as ' . $redirected_class . ', infinite loop.', { term => $class });
				}
				$self->log->info("Redirecting to $redirected_class");
				$run_query->($q, $redirected_class, $directives, $cb);
			}
			else {
				die($e);
			}
		};
	};
	
	try {
		$run_query->($q, sub {
			my $q = shift;
			foreach my $warning ($self->all_warnings){
				push @{ $q->warnings }, $warning;
			}
			
			if ($q->batch){
				$q->dedupe_warnings();
				$cb->($q);
			}
			else {
				$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
		
				#$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000));
		
				# Apply transforms
				$q->transform_results(sub { 
					$q->dedupe_warnings();
					$cb->($q);
				});
			}
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub get_log_info {
	my ($self, $args, $cb) = @_;
	my $user = $args->{user};
	$args->{q} =~ s/ /+/g;
	
	my $decode;
	eval {
		$decode = $self->json->decode(decode_base64($args->{q}));
	};
	if ($@){
		throw(400, 'Invalid JSON args: ' . Dumper($args) . ': ' . $@);
	}
	
	unless ($decode and ref($decode) eq 'HASH'){
		$self->_error('Invalid args: ' . Dumper($decode));
		return;
	}
	$self->log->trace('decode: ' . Dumper($decode));
	
	my $data;
	my $plugins = [];
	
	# Check to see if any connectors (external apps) are available and include
	if ($self->conf->get('connectors')){
		foreach my $conn (keys %{ $self->conf->get('connectors') }){
			unshift @$plugins, 'send_to_' . $conn;
		}
	}
	
	# Get local in case the plugin needs that
	my $remote_ip;
	foreach my $key (qw(srcip dstip ip)){
		if (exists $decode->{$key} and not $self->_check_local($decode->{$key})){
			$remote_ip = $decode->{$key};
			$self->log->debug('remote_ip: ' . $key . ' ' . $remote_ip);
			last;
		}
	}
		
	unless ($decode->{class} and $self->conf->get('plugins/' . $decode->{class})){
		# Check to see if there is generic IP information for use with pcap
		if ($self->conf->get('pcap_url') or $self->conf->get('streamdb_url') or $self->conf->get('streamdb_urls')){
			my %ip_fields = ( srcip => 1, dstip => 1, ip => 1);
			foreach my $field (keys %$decode){
				if ($ip_fields{$field}){
					my $plugin = Info::Pcap->new(conf => $self->conf, data => $decode);
					push @$plugins, @{ $plugin->plugins };
					$cb->({ summary => $plugin->summary, urls => $plugin->urls, plugins => $plugins, remote_ip => $remote_ip });
					return;
				}
			}
		}
		
		$self->log->debug('no plugins for class ' . $decode->{class});
		$data =  { summary => 'No info.', urls => [], plugins => $plugins };
		$cb->($data);
		return;
	}
	
	eval {
		my $plugin = $self->conf->get('plugins/' . $decode->{class})->new(conf => $self->conf, data => $decode);
		push @$plugins, @{ $plugin->plugins };
		$data =  { summary => $plugin->summary, urls => $plugin->urls, plugins => $plugins, remote_ip => $remote_ip };
	};
	if ($@){
		my $e = $@;
		throw(500, 'Error creating plugin ' . $self->conf->get('plugins/' . $decode->{class}) . ': ' . $e);
	}
		
	unless ($data){
		throw(404, 'Unable to find info from args: ' . Dumper($decode));
	}
		
	$cb->($data);
}

sub _check_local {
	my $self = shift;
	my $ip = shift;
	my $ip_int = unpack('N*', inet_aton($ip));
	
	my $subnets = $self->conf->get('transforms/whois/known_subnets');
	return unless $ip_int and $subnets;
	
	foreach my $start (keys %$subnets){
		if (unpack('N*', inet_aton($start)) <= $ip_int 
			and unpack('N*', inet_aton($subnets->{$start}->{end})) >= $ip_int){
			return 1;
		}
	}
}

sub _verify_fields_exist {
	my $self = shift;
	my $q = shift;
	my $index = shift;
	
	foreach my $boolean (qw(and or not)){
		foreach my $class_id (keys %{ $q->terms->{field_terms}->{$boolean} }){
			RAW_FIELD_LOOP: foreach my $raw_field (keys %{ $q->terms->{field_terms}->{$boolean}->{$class_id} }){
				foreach my $field_available (@{ $index->{schema}->{fields} }){
					if ($raw_field eq $field_available){
						next RAW_FIELD_LOOP;
					}
				}
				# This field wasn't found
				return $raw_field;
			}
		}
	}
	# All were found
	return undef;
}

sub get_bulk_file {
	my ($self, $args, $cb) = @_;
	
	if ( $args and ref($args) eq 'HASH' and $args->{qid} and $args->{name} ) {
		my ($query, $sth);
		$query = 'SELECT qid FROM query_log WHERE qid=? AND uid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid}, $args->{user}->uid);
		my $row = $sth->fetchrow_hashref;
		unless ($row){
			$self->log->error('No qid ' . $args->{qid} . ' for user ' . Dumper($args->{user}->username));
			return 'No query found for that id for this user';
		}
		
		my $file = Results::get_bulk_file($args->{name});
		throw(404, 'File ' . $file . ' not found', { bulk_file => $file }) unless -f $file;
		open($args->{bulk_file_handle}, $file) or throw(404, $!, { bulk_file => $file });
		
		$cb->({ 
			ret => $args->{bulk_file_handle}, 
			mime_type => 'text/plain',
			filename => $args->{name},
		});
	}
	else {
		$self->log->error('Invalid args: ' . Dumper($args));
		throw(400, 'Unable to build results object from args');
	}
}

sub format_results {
	my ($self, $args, $cb) = @_;
	
	my $ret = '';
	if ($args->{format} eq 'tsv'){
		if ($args->{groupby}){
			foreach my $groupby (@{ $args->{groupby} }){
				foreach my $row (@{ $args->{results}->{$groupby} }){
					print join("\t", $row->{'_groupby'}, $row->{'_count'}) . "\n";
				}
			}
		}
		else {
			my @default_columns = qw(timestamp class host program msg);
			$ret .= join("\t", @default_columns, 'fields') . "\n";
			foreach my $row (@{ $args->{results} }){
				my @tmp;
				foreach my $key (@default_columns){
					push @tmp, $row->{$key} if defined $row->{$key};
				}
				my @fields;
				foreach my $field (@{ $row->{_fields} }){
					push @fields, ($field->{field} ? $field->{field} : '') . '=' . ($field->{value} ? $field->{value} : '');
				}
				$ret .= join("\t", @tmp, join(' ', @fields)) . "\n";
			}
		}
	}
	elsif ($args->{format} eq 'flat_json'){
		my $json = [];
		if ($args->{groupby}){
			$json = $args->{results};
		}
		else {
			foreach my $row (@{ $args->{results} }){
				foreach my $field (@{ $row->{_fields} }){
					next if $field->{class} eq 'any' or not $field->{class};
					$row->{ $field->{class} . '.' . $field->{field} } = $field->{value};
				}
				delete $row->{_fields};
				push @$json, $row;
			}
		}
		$ret = $self->json->encode($json);
	}
	else {
		# default to JSON
		$ret .= $self->json->encode($args->{results}) . "\n";
	}
	$cb->($ret);
}

sub export {
	my ($self, $args, $cb) = @_;
	
	if ( $args and ref($args) eq 'HASH' and $args->{data} and $args->{plugin} ) {
		my $decode;
		eval {
			$decode = $self->json->decode(uri_unescape($args->{data}));
			$self->log->debug( "Decoded data as : " . Dumper($decode) );
			if (ref($decode) eq 'HASH' and $decode->{qid}){
				$decode->{user} = $args->{user};
				$decode = $self->get_saved_result($decode)->{results};
			}
		};
		if ($@){
			$self->log->error("invalid args, error: $@, args: " . Dumper($args));
			return 'Unable to build results object from args';
		}
		
		my $results_obj;
		foreach my $plugin ($self->export_plugins()){
			if ($plugin =~ /\:\:$args->{plugin}$/i){
				$self->log->debug('loading plugin ' . $plugin);
				$results_obj = $plugin->new(results => $decode);
				$self->log->debug('results_obj:' . Dumper($results_obj));
			}
		}
		if ($results_obj){
			$cb->({ 
				ret => $results_obj->results(), 
				mime_type => $results_obj->mime_type(), 
				filename => CORE::time() . $results_obj->extension,
			});
			return;
		}
		
		$self->log->error("failed to find plugin " . $args->{plugin} . ', only have plugins ' .
			join(', ', $self->export_plugins()) . ' ' . Dumper($args));
			throw(400, 'Unable to build results object from args');
	}
	else {
		$self->log->error('Invalid args: ' . Dumper($args));
		throw(400, 'Unable to build results object from args');
	}
}

sub send_to {
	my ($self, $args, $cb) = @_;
	
	if (ref($args) =~ /Query/){
		my $q = $args;
		$cb->() and return unless $q->has_connectors;
		$self->_send_to($q, $cb);
	}
	else {
		if($args->{query}){
			$self->log->debug('args: ' . Dumper($args));
			QueryParser->new(conf => $self->conf, user => $args->{user}, query_string => $args->{query}->{query_string}, 
				connectors => $args->{connectors}, meta_params => $args->{query}->{query_meta_params}, 
				qid => $args->{qid} ? $args->{qid} : 0, on_connect => sub {
					my $qp = shift;
					my $q = $qp->parse();
					$q->results->add_results($args->{results});
					$cb->() and return unless $q->has_connectors;
					$self->_send_to($q, $cb);
				});
			
		}
		else {
			throw(400, 'Invalid args, no Query');
		}
	}
}

sub _send_to {		
	my $self = shift;
	my $q = shift;
	my $cb = shift;

	for (my $i = 0; $i < $q->num_connectors; $i++){
		my $raw = $q->connector_idx($i);
		my ($connector, @connector_args);
		if ($raw =~ /(\w+)\(([^\)]+)?\)/){
			$connector = $1;
			if ($2){
				@connector_args = split(/\,/, $2);
			}
			elsif ($q->connector_params_idx($i)){
				@connector_args = $q->connector_params_idx($i);
				$self->log->debug('connector_params: ' . Dumper($q->connector_params));
				$self->log->debug('set @connector_args to ' . Dumper(\@connector_args)); 
			}
		}
		else {
			$connector = $raw;
			my $cargs = $q->connector_params_idx($i);
			@connector_args = @{ $cargs } if $cargs;
		}
		
		my $num_found = 0;
		foreach my $plugin ($self->connector_plugins()){
			if ($plugin =~ /\:\:$connector(?:\:\:|$)/i){
				$self->log->debug('loading plugin ' . $plugin);
				eval {
					# Check to see if we are processing bulk results
					if ($q->results->is_bulk){
						$q->results->close();
						my $ret_results = new Results();
						while (my $results = $q->results->get_results(0,$Query::Max_limit)){
							last unless scalar @$results;
							my $plugin_object = $plugin->new(
								controller => $self,
								user => $q->user,
								results => { results => $results },
								args => [ @connector_args ],
								query_schedule_id => $q->schedule_id,
								comments => $q->comments,
							);
							$ret_results->add_results($plugin_object->results->{results});
							# for returnable amount
							if (ref($plugin_object->results) eq 'HASH'){
								#$self->log->trace('returnable results: ' . scalar @{ $plugin_object->results->{results} });
								foreach (@{ $plugin_object->results->{results} }){
									last if ($q->limit and $ret_results->total_records > $q->limit) 
										or ($ret_results->total_records > $Query::Max_limit);
									$ret_results->add_result($_);
								}
							}
							else {
								#$self->log->trace('returnable results: ' . scalar @{ $plugin_object->results });
								foreach (@{ $plugin_object->results }){
									last if ($q->limit and $ret_results->total_records > $q->limit) 
										or ($ret_results->total_records > $Query::Max_limit);
									$ret_results->add_result($_);
								}
							}
						}
						$ret_results->close();
						$q->results($ret_results);
					}
					else {
						my $plugin_object = $plugin->new(
							controller => $self,
							user => $q->user,
							results => { results => $q->results->results },
							args => [ @connector_args ],
							query => $q,
						);
						if ($q->has_groupby and ref($plugin_object->results) eq 'HASH' and scalar keys %{ $plugin_object->results }){
							$q->results(Results::Groupby->new(results => $plugin_object->results->{results}));
						}
						elsif (ref($plugin_object->results) eq 'HASH' and scalar keys %{ $plugin_object->results }){
							$q->results(Results->new(results => $plugin_object->results->{results}));
						}
						elsif (ref($plugin_object->results) eq 'ARRAY'){
							$q->results(Results->new(results => $plugin_object->results));
						}
					}
					$num_found++;
				};
				if ($@){
					$self->log->error('Error creating plugin ' . $plugin . ' with data ' 
						. Dumper($q->results) . ' and args ' . Dumper(\@connector_args) . ': ' . $@);
					return [ 'Error: ' . $@ ];
				}
			}
		}
		unless ($num_found){
			$self->log->error("failed to find connectors " . Dumper($q->connectors) . ', only have connectors ' .
				join(', ', $self->connector_plugins()));
			$cb->(0);
			return;
		}
	}
	#$self->log->debug('$q->results->all_results: ' . Dumper($q->results->all_results));
	
	$cb->([ $q->results->all_results ]);
}

sub _check_foreign_queries {
	my $self = shift;
	my $cb = shift;
	
	$self->log->trace('Checking foreign queries');
	my ($query, $sth);
	
	$query = 'SELECT DISTINCT qid FROM foreign_queries WHERE ISNULL(completed)';
	$sth = $self->db->prepare($query);
	$sth->execute;
	my @incomplete;
	while (my $row = $sth->fetchrow_hashref){
		push @incomplete, $row;
	}
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub{ $cb->(); });
	foreach my $row (@incomplete){
		$cv->begin;
		eval {
			$self->result({ qid => $row->{qid} }, sub {
				my $result = shift;
				if ($result and not $result->{incomplete}){
					# overwrite the foreign_qid with our local qid
					$result->{qid} = $row->{qid};
					$self->_save_results($result);
					$self->_batch_notify($result);
				}
				$cv->end;
			});
		};
		if ($@){
			$self->log->error('Error getting result for qid ' . $row->{qid} . ': ' . $@);
			$cv->end;
		}
	}
	$cv->end;
}

sub send_email {
	my ($self, $args, $cb) = @_;
	
	unless ($args->{user} eq 'system'){
		throw(403, 'Insufficient permissions', { admin => 1 });
	}
	
	# Send the email
	my $email_headers = new Mail::Header();
	$email_headers->header_hashref($args->{headers});
	my $email = new Mail::Internet( Header => $email_headers, Body => [ split(/\n/, $args->{body}) ] );
	
	$self->log->debug('email: ' . $email->as_string());
	my $ret;
	if ($self->conf->get('email/smtp_server')){
		$ret = $email->smtpsend(
			Host => $self->conf->get('email/smtp_server'), 
			Debug => 1, 
			MailFrom => $self->conf->get('email/display_address')
		);
	}
	else {
		($ret) = Email::LocalDelivery->deliver($email->as_string, $self->conf->get('logdir') . '/' . $self->conf->get('email/to'));
	}
	if ($ret){
		$self->log->debug('done sending email');
		$cb->(1);
	}
	else {
		$self->log->error('Unable to send email: ' . $email->as_string());
		$cb->(0);
	}
}

sub _batch_notify {
	my ($self, $q) = @_;
	#$self->log->trace('got results for batch: ' . Dumper($args));
	
	my $num_records = $q->results->total_records ? $q->results->total_records : $q->results->records_returned;
	my $headers = {
		To => $q->user->email,
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => sprintf('ELSA archive query %d complete with %d results', $q->qid, $num_records),
	};
	my $body;
	
	if ($q->results->is_bulk){
		$body = sprintf('%d results for query %s', $num_records, $q->query_string) .
			"\r\n" . sprintf('%s/Query/get_bulk_file?qid=%d&name=%s', 
				$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
				$q->qid, $q->results->bulk_file->{name});
	}
	else {
		$body = sprintf('%d results for query %s', $num_records, $q->query_string) .
			"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
				$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
				$q->qid, $q->hash);
	}
	
	$self->send_email({ headers => $headers, body => $body, user => 'system'}, sub {});
}

sub cancel_query {
	my ($self, $args, $cb) = @_;
	
	my ($query, $sth);
	if ($args->{user}){
		$query = 'UPDATE query_log SET num_results=-2 WHERE qid=? AND uid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid}, $args->{user}->uid);
	}
	else {
		$query = 'UPDATE query_log SET num_results=-2 WHERE qid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid});
	}
	
	# Recursively cancel queries run on our behalf from other nodes
	$self->log->trace('Cancelling foreign queries');
	
	$query = 'SELECT peer, foreign_qid FROM foreign_queries WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	
	my %peers;
	while (my $row = $sth->fetchrow_hashref){
		$peers{ $row->{peer} } = $row->{foreign_qid};
	}
	$self->log->debug('peers: ' . Dumper(\%peers));
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$query = 'DELETE FROM foreign_queries WHERE qid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid});		
		$cb->({ ok => ((scalar keys %peers) + 1) });
	});
	
	my $headers = { 'Content-type' => 'application/x-www-form-urlencoded', 'User-Agent' => $self->user_agent_name };
	foreach my $peer (sort keys %peers){
		my $peer_label = $peer;
		$cv->begin;
		
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/cancel_query';
		my $request_body = 'qid=' . $peers{$peer};
		$self->log->trace('Sending request to URL ' . $url . ' with body ' . $request_body);
		my $start = time();
		
		if ($peer_conf->{headers}){
			foreach my $header_name (keys %{ $peer_conf->{headers} }){
				$headers->{$header_name} = $peer_conf->{headers}->{$header_name};
			}
		}
		$headers->{Authorization} = $self->_get_auth_header($peer);
		# Do not proxy localhost requests
		my @no_proxy = ();
		if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
			push @no_proxy, proxy => undef;
		}
		$peers{$peer} = http_post $url, $request_body, headers => $headers, @no_proxy, sub {
			my ($body, $hdr) = @_;
			#$self->log->debug('body: ' . Dumper($body));
			unless ($hdr and $hdr->{Status} < 400){
				my $e;
				try {
					my $raw = $self->json->decode($body);
					$e = bless($raw, 'Ouch') if $raw->{code} and $raw->{message};
				};
				$e ||= new Ouch(500, 'Internal error');
				$self->log->error('Peer ' . $peer_label . ' got error: ' . $e->trace);
				$self->add_warning($e->code, $e->message, $e->data);
				delete $peers{$peer};
				$cv->end;
				return;
			}
			eval {
				my $raw_results = $self->json->decode($body);
				if ($raw_results and ref($raw_results) and $raw_results->{error}){
					$self->log->error('Peer ' . $peer_label . ' got error: ' . $raw_results->{error});
					$self->add_warning(502, 'Peer ' . $peer_label . ' encountered an error.', { http => $peer });
					return;
				}
				else {
					$self->log->info('Cancelled query on peer ' . $peer);
				}
			};
			if ($@){
				$self->log->error($@ . 'url: ' . $url . "\nbody: " . $request_body);
				$self->add_warning(502, 'Invalid results back from peer ' . $peer_label, { http => $peer });
			}	
			delete $peers{$peer};
			$cv->end;
		};
	}
	$cv->end;
}

sub preference {
	my ($self, $args, $cb) = @_;
	
	throw(400, 'No user', { user => 1 }) unless $args->{user};
	
	my ($query, $sth);
	
	# Lower case these vars
	for my $var (qw(type name value)){
		if (exists $args->{$var}){
			$args->{$var} = lc($args->{$var});
		}
	}
	
	if ($args->{action} eq 'add'){
		$query = 'INSERT INTO preferences (uid, type, name, value) VALUES (?,?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{type}, $args->{name}, $args->{value});
	}
	elsif ($args->{action} eq 'remove'){
		$query = 'DELETE FROM preferences WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{id});
	}
	elsif ($args->{action} eq 'update'){
		throw(400, 'Need col/val', { col => 1 }) unless $args->{col} and defined $args->{val};
		if ($args->{col} eq 'name'){
			$query = 'UPDATE preferences SET name=? WHERE id=? AND uid=?';
		}
		elsif ($args->{col} eq 'value'){
			$query = 'UPDATE preferences SET value=? WHERE id=? AND uid=?';
		}	
		$sth = $self->db->prepare($query);
		$sth->execute($args->{val}, $args->{id}, $args->{user}->uid);
	}
	else {
		throw(404, 'Invalid action', { action => 1 });
	}
	
	$cb->({ ok => $sth->rows });
}

__PACKAGE__->meta->make_immutable;
1;
