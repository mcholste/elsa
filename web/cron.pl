#!/usr/bin/perl
use strict;
use Getopt::Std;
use Time::HiRes qw(time);
use FindBin;
use AnyEvent;
use Data::Dumper;
use Date::Manip;

use lib $FindBin::Bin . '/../node/';
use lib $FindBin::Bin . '/lib';

use Indexer;
use Controller;
use User;

my %opts;
getopts('c:n:', \%opts);

my $Max_concurrent_archive_queries = 2;

my $config_file;
if ($opts{c}){
	$config_file = $opts{c};
}
elsif ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}
else {
	$config_file = '/etc/elsa_web.conf';
}
die('Cannot find config file, specify with -c or env variable ELSA_CONF') unless -f $config_file;

my $node_config_file;
if ($opts{n}){
	$node_config_file = $opts{n};
}
elsif ($ENV{ELSA_NODE_CONF}){
	$node_config_file = $ENV{ELSA_NODE_CONF};
}
else {
	$node_config_file = '/etc/elsa_node.conf';
}
die('Cannot find node config file, specify with -n or env variable ELSA_NODE_CONF') unless -f $node_config_file;

#$ENV{DEBUG_LEVEL} = 'INFO'; # we don't want to fill our logs up with automated query logs

my $indexer = Indexer->new(config_file => $node_config_file);
if ($indexer->conf->get('debug_all')){
	$ENV{DEBUG_LEVEL} = 'TRACE';
	$indexer->log->level($ENV{DEBUG_LEVEL});
}

eval {
	# Handle node activities, like loading buffers
	print "Indexing buffers...\n";
	$indexer->load_buffers() or return;
	print "...finished.\n";
	
	# Attempt to get a lock to ensure there are no other cron.pl's querying right now
	unless($indexer->_get_lock('query', 1)){
		my $msg = 'Another cron.pl script is querying, exiting';
		warn $msg;
		$indexer->log->error($msg);
		exit;
	}
	
	# Handle web activities, like scheduled searches
	$Log::Log4perl::Logger::INITIALIZED = 0; #deinit log4perl se we can re-init here
	my $controller = Controller->new(config_file => $config_file) or die('Unable to start from given config file.');
	return unless _need_to_run($controller);
	my $start = time();
	my $cv = AnyEvent->condvar;
	$cv->begin;
	$controller->get_form_params(undef, sub {
		my $form_params = shift;
		my $cur_time = $form_params->{end_int};
		my $query_params = _get_schedule($controller, $cur_time);
		my $num_run = _run_schedule($controller, $query_params, $cur_time);
		
		my $duration = time() - $start;
		$controller->log->trace("Ran $num_run queries in $duration seconds.");
		
		# Unlock so that the next cron.pl can make schedule queries
		$indexer->_release_lock('query');
		
		# Archive queries are expected to take a long time and can run concurrently
		$controller->log->trace("Running archive queries...");
		_run_archive_queries($controller, $cv);
		$cv->end;
	});
	$cv->recv;
};
if ($@){
	warn('Error: ' . $@);
	$indexer->log->error('Error: ' . $@);
}

$indexer->log->trace('cron.pl finished.');

sub _need_to_run {
	my $controller = shift;
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*)' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users ON (t1.uid=users.uid)' . "\n" .
		'WHERE enabled=1' . "\n" .
		'AND UNIX_TIMESTAMP() - UNIX_TIMESTAMP(last_alert) > alert_threshold';  # we won't even run queries we know we won't alert on
	$sth = $controller->db->prepare($query);
	$sth->execute;
	my $row = $sth->fetchrow_arrayref;
	my $count = $row->[0];
	
	$query = 'SELECT COUNT(*) FROM query_log WHERE ISNULL(num_results) AND archive=1';
	$sth = $controller->db->prepare($query);
	$sth->execute;
	$row = $sth->fetchrow_arrayref;
	$count += $row->[0];
	
	$query = 'SELECT COUNT(*) FROM foreign_queries WHERE ISNULL(completed)';
	$sth = $controller->db->prepare($query);
	$sth->execute;
	$row = $sth->fetchrow_arrayref;
	$count += $row->[0];
	
	return $count;
}

sub _get_schedule {
	my $controller = shift;
	my $cur_time = shift;
	
	my ($query, $sth);
	
	# Find the last run time from the bookmark table
	$query = 'SELECT UNIX_TIMESTAMP(last_run) FROM schedule_bookmark';
	$sth = $controller->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_arrayref;
	my $last_run_bookmark = $controller->conf->get('schedule_interval'); # init to interval here so we don't underflow if 0
	if ($row){
		$last_run_bookmark = $row->[0];
	}
	# Run schedule	
	$query = 'SELECT t1.id AS query_schedule_id, username, t1.uid, query, frequency, start, end, connector, params' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users ON (t1.uid=users.uid)' . "\n" .
		'WHERE start <= ? AND end >= ? AND enabled=1' . "\n" .
		'AND UNIX_TIMESTAMP() - UNIX_TIMESTAMP(last_alert) > alert_threshold';  # we won't even run queries we know we won't alert on
	
	$sth = $controller->db->prepare($query);
	$sth->execute($cur_time, $cur_time);
	
	my $user_info_cache = {};
	
	my @query_params;
	while (my $row = $sth->fetchrow_hashref){
		my @freq_arr = split(':', $row->{frequency});
		my $last_run;
		my $farthest_back_to_check = $cur_time - $controller->conf->get('schedule_interval');
		my $how_far_back = $controller->conf->get('schedule_interval');
		while (not $last_run and $farthest_back_to_check > ($cur_time - (86400 * 366 * 2))){ # sanity check
			$controller->log->debug('$farthest_back_to_check:' . $farthest_back_to_check);
			my @prev_dates = ParseRecur($row->{frequency}, 
				ParseDate(scalar localtime($cur_time)), 
				ParseDate(scalar localtime($farthest_back_to_check)),
				ParseDate(scalar localtime($cur_time - 1))
			);
			if (scalar @prev_dates){
				$controller->log->trace('prev: ' . Dumper(\@prev_dates));
				$last_run = UnixDate($prev_dates[$#prev_dates], '%s');
				$controller->log->trace('last_run:' . $prev_dates[$#prev_dates]);
			}
			else {
				# Keep doubling the distance we'll go back to find the last date
				$farthest_back_to_check -= $how_far_back;
				$controller->log->trace('how_far_back: ' . $how_far_back);
				$how_far_back *= 2;
			}
		}
		unless ($last_run){
			$controller->log->error('Could not find the last time we ran, aborting');
			next;
		}
		# If the bookmark is earlier, use that because we could've missed runs between them
		if ($last_run_bookmark < $last_run){
			$controller->log->info('Setting last_run to ' . $last_run_bookmark . ' because it is before ' . $last_run);
			$last_run = $last_run_bookmark;
		}
		my @dates = ParseRecur($row->{frequency}, 
			ParseDate("jan 1"), # base time to use for the recurrence period 
			ParseDate(scalar localtime($cur_time)),
			ParseDate(scalar localtime($cur_time + $controller->conf->get('schedule_interval')))
		);
		$controller->log->trace('dates: ' . Dumper(\@dates) . ' row: ' . Dumper($row));
		if (scalar @dates){
			# Adjust the query time to avoid time that is potentially unindexed by offsetting by the schedule interval
			my $query_params = $controller->json->decode($row->{query});
			$query_params->{meta_params} = delete $query_params->{query_meta_params};
			$query_params->{meta_params}->{start} = ($last_run - $controller->conf->get('schedule_interval'));
			$query_params->{meta_params}->{end} = ($cur_time - $controller->conf->get('schedule_interval'));
			$query_params->{schedule_id} = $row->{query_schedule_id};
			$query_params->{connectors} = [ $row->{connector} ];
			$query_params->{system} = 1; # since the user did not init this, it's a system query
			$controller->log->debug('query_params: ' . Dumper($query_params));
			
			if (!$user_info_cache->{ $row->{uid} }){
				$user_info_cache->{ $row->{uid} } = $controller->get_user($row->{username});
				#$controller->log->trace('Got user info: ' . Dumper($user_info_cache->{ $row->{uid} }));
			}
			else {
				#$controller->log->trace('Using existing user info');
			}
			$query_params->{user} = $user_info_cache->{ $row->{uid} };
			push @query_params, $query_params;
		}
	}
	return \@query_params;
}

sub _run_schedule {
	my $controller = shift;
	my $query_params = shift;
	my $cur_time = shift;
	
	my ($query, $sth);
	
	# Expire schedule entries
	$query = 'SELECT id, query, username FROM query_schedule JOIN users ON (query_schedule.uid=users.uid) WHERE end < UNIX_TIMESTAMP() AND enabled=1';
	$sth = $controller->db->prepare($query);
	$sth->execute();
	my @ids;
	while (my $row = $sth->fetchrow_hashref){
		push @ids, $row->{id};
		my $user = $controller->get_user($row->{username});
		my $decode = $controller->json->decode($row->{query});
		
		my $headers = {
			To => $user->email,
			From => $controller->conf->get('email/display_address') ? $controller->conf->get('email/display_address') : 'system',
			Subject => 'ELSA alert has expired for query ' . $decode->{query_string},
		};
		my $body = 'The alert set for query ' . $decode->{query_string} . ' has expired and has been disabled.  ' .
			'If you wish to continue receiving this query, please log into ELSA, enable the query, and set a new expiration date.';
		
		$controller->send_email({headers => $headers, body => $body, user => 'system'}, sub {});
	}
	if (scalar @ids){
		$controller->log->info('Expiring query schedule for ids ' . join(',', @ids));
		$query = 'UPDATE query_schedule SET enabled=0 WHERE id IN (' . join(',', @ids) . ')';
		$sth = $controller->db->prepare($query);
		$sth->execute;
	}
	
	# Perform queries
	my $counter = 0;
	foreach my $query_params (@$query_params){
		eval {
			$controller->query($query_params, sub { $counter++ });
		};
		if ($@){
			$controller->log->error('Problem running query: ' . Dumper($query_params) . "\n" . $@);
		}
	}
	
	# Verify we've received logs from hosts specified in the config file
	if ($controller->conf->get('host_checks')){
		my $admin_email = $controller->conf->get('admin_email_address');
		if ($admin_email){
			my %intervals;
			foreach my $host (keys %{ $controller->conf->get('host_checks') }){
				my $interval = $controller->conf->get('host_checks')->{$host};
				$intervals{$interval} ||= [];
				push @{ $intervals{$interval} }, $host;
			}
			
			# For each unique interval, run all the hosts in a batch via groupby:host
			foreach my $interval (keys %intervals){
				my $query_params = { 
					query_string => join(' ', map { 'host:' . $_ } @{ $intervals{$interval} }) . ' groupby:host', 
					meta_params => { 
						start => (time() - $interval - 60), # 60 second grace period for batch load
						limit => scalar @{ $intervals{$interval} },
					},
					system => 1,
				};
				$controller->log->debug('query_params: ' . Dumper($query_params));
				$controller->query($query_params, sub {
					my $q = shift;
					my %not_found = map { $_ => 1 } @{ $intervals{$interval} };
					foreach my $row (@{ $q->results->all_results }){
						$controller->log->trace('Found needed results for ' . $row->{_groupby} . ' in interval ' . $interval);
						delete $not_found{ $row->{_groupby} };
					}
					foreach my $host (keys %not_found){
						my $errmsg = 'Did not find entries for host ' . $host . ' within interval ' . $interval;
						$controller->log->error($errmsg);
						my $headers = {
							To => $controller->conf->get('admin_email_address'),
							From => $controller->conf->get('email/display_address') ? $controller->conf->get('email/display_address') : 'system',
							Subject => sprintf('Host inactivity alert: %s', $host),
						};
						$controller->send_email({ headers => $headers, body => $errmsg, user => 'system' });
					}
				});
			}
		}
		else {
			$controller->log->error('Configured to do host checks via host_checks but no admin_email_address found in config file');
		}	
	}
	
	# Update our bookmark to the current run
	$query = 'UPDATE schedule_bookmark SET last_run=FROM_UNIXTIME(?)';
	$sth = $controller->db->prepare($query);
	$sth->execute($cur_time);
	unless ($sth->rows){
		$query = 'INSERT INTO schedule_bookmark (last_run) VALUES (FROM_UNIXTIME(?))';
		$sth = $controller->db->prepare($query);
		$sth->execute($cur_time);
	}
	
	return $counter;
}

sub _run_archive_queries {
	my $controller = shift;
	my $cv = shift;
	
	my ($query, $sth);
	
	# Expire any failed archive queries
	$query = 'SELECT qid, pid FROM query_log WHERE archive=1 AND NOT ISNULL(pid) AND num_results=-1';
	$sth = $controller->db->prepare($query);
	$sth->execute();
	my @running;
	while (my $row = $sth->fetchrow_hashref){
		push @running, $row;
	}
	
	
	foreach my $row (@running){
		if (not $indexer->_find_pid($row->{pid})){
			$controller->log->error('Found dead pid ' . $row->{pid} . ' for archive query ' . $row->{qid} . ', abandoning');
			
			# Mark this done
			$query = 'UPDATE query_log SET num_results=0 WHERE qid=?';
			$sth = $controller->db->prepare($query);
			$sth->execute($row->{qid});
			
			# Make sure we don't try to keep updating foreign query results either
			$query = 'UPDATE foreign_queries SET completed=UNIX_TIMESTAMP() WHERE qid=?';
			$sth = $controller->db->prepare($query);
			$sth->execute($row->{qid});
		}
	}
	
	# Only run one query per user, and only if that user isn't already running a query
	$query = 'SELECT MIN(qid) AS qid, username, query, t1.uid FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) ' .
		'WHERE ISNULL(num_results) AND archive=1 AND t1.uid NOT IN (SELECT DISTINCT uid FROM query_log WHERE num_results=-1) ' . 
		'GROUP BY username ORDER BY qid ASC LIMIT ?';
	$sth = $controller->db->prepare($query);
	$sth->execute($Max_concurrent_archive_queries);
	
	while (my $row = $sth->fetchrow_hashref){
		my $user = $controller->get_user($row->{username});
		$cv->begin;
		$controller->local_query({ user => $user, q => $row->{query}, qid => $row->{qid} }, 1, sub {
			my $q = shift;
			# Record the results
			$controller->log->trace('got archive results: ' . $q->results->total_records);
			$controller->_save_results($q->TO_JSON);
			$controller->_batch_notify($q);

			$query = 'UPDATE query_log SET milliseconds=? WHERE qid=?';
			my $upd_sth = $controller->db->prepare($query);
			$upd_sth->execute($q->time_taken, $row->{qid});
			$controller->log->trace('Updated num_results for qid ' . $row->{qid} 
				. ' with ' . $q->results->records_returned . ' additional records.');
			$upd_sth->finish;
			
			# Check foreign queries
			$controller->result({ qid => $row->{qid} }, 1, sub {
				$cv->end;
			});
		});
	}
	
	$cv->begin;
	$controller->_check_foreign_queries(sub { $cv->end });
}
