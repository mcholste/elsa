package Utils;
use Data::Dumper;
use Moose::Role;
with 'MooseX::Log::Log4perl';
use Config::JSON;
use DBI;
use JSON;
use IO::Handle;
use IO::File;
use Digest::HMAC_SHA1;
use Socket;
use Time::HiRes qw(time);
use Hash::Merge::Simple qw(merge);
use AnyEvent::HTTP;
use URI::Escape qw(uri_escape);
use Time::HiRes qw(time);
use Digest::SHA qw(sha512_hex);
use Sys::Hostname;
use Try::Tiny;
use Ouch qw(:trytiny);;
use Exporter qw(import);
use Encode;

our @EXPORT = qw(catch_any epoch2iso unicode_escape);

use CustomLog;
use Results;

our $Db_timeout = 3;
our $Bulk_dir = '/tmp';
our $Auth_timestamp_grace_period = 86400;

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'json' => (is => 'ro', isa => 'JSON', required => 1);
#has 'bulk_dir' => (is => 'rw', isa => 'Str', required => 1, default => $Bulk_dir);
has 'db_timeout' => (is => 'rw', isa => 'Int', required => 1, default => $Db_timeout);
has 'meta_info' => (is => 'rw', isa => 'HashRef');

around BUILDARGS => sub {
	my $orig = shift;
	my $class = shift;
	my %params = @_;
	
	unless (exists $params{config_file} or exists $params{conf}){
		$params{config_file} = '/etc/elsa_web.conf';
	}
	if ($params{config_file}){
		$params{conf} = new Config::JSON ( $params{config_file} ) or die("Unable to open config file");
	}		
	
	my $log_level = 'DEBUG';
	if ($ENV{DEBUG_LEVEL}){
		$log_level = $ENV{DEBUG_LEVEL};
	}
	elsif ($params{conf}->get('debug_level')){
		$log_level = $params{conf}->get('debug_level');
	}
	my $logdir = $params{conf}->get('logdir');
	my $logfile = 'web';
	if ($params{conf}->get('logfile')){
		$logfile = $params{conf}->get('logfile');
	}
	my $tmpdir = $logdir . '/../tmp';
	
	my $log_format = 'File, RFC5424';
	if ($params{conf}->get('log_format')){
		$log_format = $params{conf}->get('log_format');
	}
	
	my $hostname = hostname;
	
	my $log_conf = qq'
		log4perl.category.App       = $log_level, $log_format
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/$logfile.log 
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Syncer            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.Syncer.appender   = File
		log4perl.appender.Dat			 = Log::Log4perl::Appender::File
		log4perl.appender.Dat.filename  = $logdir/elsa.dat
		log4perl.appender.Dat.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Dat.layout.ConversionPattern = %d{e.SSSSSS}\0%p\0%M\0%F\0%L\0%P\0%m%n\1
		log4perl.appender.SyncerDat            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.SyncerDat.appender   = Dat
		log4perl.appender.RFC5424         = Log::Log4perl::Appender::Socket::UNIX
        log4perl.appender.RFC5424.Socket = $tmpdir/ops
        #log4perl.appender.RFC5424.layout = Log::Log4perl::Layout::PatternLayout::Multiline
        log4perl.appender.RFC5424.layout = CustomLog
        log4perl.appender.RFC5424.layout.ConversionPattern = 1 %d{yyyy-MM-ddTHH:mm:ss.000}Z 127.0.0.1 elsa - 99 [elsa\@32473 priority="%p" method="%M" file="%F{2}" line_number="%L" pid="%P" client="%X{client_ip_address}" qid="%X{qid}" hostname="$hostname"] %m%n
	';
	
	if (not Log::Log4perl->initialized()){
		Log::Log4perl::init( \$log_conf ) or die("Unable to init logger");
	}
	$params{log} = Log::Log4perl::get_logger('App')
	  or die("Unable to init logger");
	
	if ($params{conf}->get('db/timeout')){
		$Db_timeout = $params{conf}->get('db/timeout');
	}
	
	$params{db} = DBI->connect_cached(
		$params{conf}->get('meta_db/dsn'),
		$params{conf}->get('meta_db/username'),
		$params{conf}->get('meta_db/password'),
		{ 
			PrintError => 0,
			HandleError => \&_dbh_error_handler,
			AutoCommit => 1,
			mysql_connect_timeout => $Db_timeout,
			mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
			mysql_local_infile => 1, # allow LOAD DATA LOCAL
		}
	) or die($DBI::errstr);
	
	if ($params{conf}->get('debug_level') eq 'DEBUG' or $params{conf}->get('debug_level') eq 'TRACE'){
		$params{json} = JSON->new->pretty->allow_nonref->allow_blessed->convert_blessed;	
	}
	else {
		$params{json} = JSON->new->allow_nonref->allow_blessed->convert_blessed;
	}
	
	if ($params{conf}->get('bulk_dir')){
		$Bulk_dir = $params{conf}->get('bulk_dir');
	}
	
	return $class->$orig(%params);
};

sub _dbh_error_handler {
	my $errstr = shift;
	my $dbh    = shift;
	my $query  = $dbh->{Statement};

	$errstr .= " QUERY: $query";
	Log::Log4perl::get_logger('App')->error($errstr);
#	foreach my $sth (grep { defined } @{$dbh->{ChildHandles}}){
#		$sth->rollback; # in case there was an active transaction
#	}
	
	throw(500, 'Internal error', { mysql => $query });
}

sub freshen_db {
	my $self = shift;
	
	try {
		$self->db->disconnect;
	};
	
	$self->db(
		DBI->connect(
			$self->conf->get('meta_db/dsn'),
			$self->conf->get('meta_db/username'),
			$self->conf->get('meta_db/password'),
			{ 
				PrintError => 0,
				HandleError => \&_dbh_error_handler,
				#RaiseError => 1,
				AutoCommit => 1,
				mysql_connect_timeout => $Db_timeout,
				mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
				mysql_local_infile => 1, # allow LOAD DATA LOCAL
			})
	);
}

sub epoch2iso {
	my $epochdate = shift;
	my $use_gm_time = shift;
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	if ($use_gm_time){
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($epochdate);
	}
	else {
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	}
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub _get_hash {
	my ($self, $data) = shift;
	my $digest = new Digest::HMAC_SHA1($self->conf->get('link_key'));
	$digest->add($data);
	return $digest->hexdigest();
}

sub _get_info {
	my $self = shift;
	my $cb = pop(@_);
	my $is_lite = shift;
	my ($query, $sth);
	
	my $overall_start = time();
	
	my $ret = { };
	$self->_get_db(sub {
		my $db = shift;
		unless ($db and $db->{dbh}){
			$self->add_warning(502, 'msyql connection error', { mysql => 1 });
			$cb->();
			return;
		}
	
		# Get info from db and call the callback with the info
		my $cv = AnyEvent->condvar;
		$cv->begin(sub {
			$cb->($ret);
		});
	
		if (exists $db->{error}){
			$self->add_warning(502, 'msyql connection error', { mysql => 1 });
			$cb->();
			return;
		}
		
		if ($is_lite){
			# Just get min/max times for indexes, count
			$query = sprintf('SELECT UNIX_TIMESTAMP(MIN(start)) AS start_int, UNIX_TIMESTAMP(MAX(end)) AS end_int, ' .
				'UNIX_TIMESTAMP(MAX(start)) AS start_max, SUM(records) AS records, type FROM %s.v_indexes ' .
				'WHERE type="temporary" OR (type="permanent" AND ISNULL(locked_by)) OR type="realtime"', $db->{db});
			$cv->begin;
			$self->log->trace($query);
			$db->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$ret->{indexes} = {
						min => $rows->[0]->{start_int} < $overall_start ? $rows->[0]->{start_int} : 0,
						max => $rows->[0]->{end_int} < $overall_start ? $rows->[0]->{end_int} : $overall_start,
						start_max => $rows->[0]->{start_max} < $overall_start ? $rows->[0]->{start_max} : 0,
						records => $rows->[0]->{records},
					};
				}
				else {
					$self->log->error('No indexes, rv: ' . $rv);
					$ret->{error} = 'No indexes';
				}
				$cv->end;
			});
		}
		else {		
			# Get indexes
			$query = sprintf('SELECT CONCAT(SUBSTR(type, 1, 4), "_", id) AS name, start AS start_int, FROM_UNIXTIME(start) AS start,
			end AS end_int, FROM_UNIXTIME(end) AS end, type, last_id-first_id AS records, index_schema
			FROM %s.indexes WHERE type="temporary" OR (type="permanent" AND ISNULL(locked_by)) OR type="realtime" ORDER BY start', 
				$db->{db});
			$cv->begin;
			$self->log->trace($query);
			$db->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					foreach my $row (@$rows){
						$row->{schema} = decode_json(delete $row->{index_schema}) if $row->{index_schema};
					}
					$ret->{indexes} = {
						indexes => $rows,
						min => $rows->[0]->{start_int} < $overall_start ? $rows->[0]->{start_int} : 0,
						max => $rows->[$#$rows]->{end_int} < $overall_start ? $rows->[$#$rows]->{end_int} : $overall_start,
						start_max => $rows->[$#$rows]->{start_int} < $overall_start ? $rows->[0]->{start_int} : 0,
					};
				}
				else {
					$self->log->error('No indexes, rv: ' . $rv);
					$ret->{error} = 'No indexes';
				}
				$cv->end;
			});
		}
		
		if ($is_lite){
			# Just get min/max times, count
			$query = sprintf('SELECT UNIX_TIMESTAMP(MIN(start)) AS start_int, ' .
				'UNIX_TIMESTAMP(MIN(end)) AS end_int, SUM(max_id - min_id) AS records ' .
				'FROM %s.tables t1 JOIN table_types t2 ON (t1.table_type_id=t2.id) WHERE t2.table_type="archive"', 
				$db->{db});
			$cv->begin;
			$self->log->trace($query);
			$db->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$ret->{tables} = {
						min => $rows->[0]->{start_int},
						max => $rows->[0]->{end_int},
						start_max => $rows->[0]->{start_int},
						records => $rows->[0]->{records},
					};
				}
				else {
					$self->log->error('No tables');
					$ret->{error} = 'No tables';
				}
				$cv->end;
			});
		}
		else {
			# Get tables
			$query = sprintf('SELECT table_name, start, UNIX_TIMESTAMP(start) AS start_int, end, ' .
				'UNIX_TIMESTAMP(end) AS end_int, table_type, min_id, max_id, max_id - min_id AS records ' .
				'FROM %s.tables t1 JOIN table_types t2 ON (t1.table_type_id=t2.id) ORDER BY start', 
				$db->{db});
			$cv->begin;
			$self->log->trace($query);
			$db->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$ret->{tables} = {
						tables => $rows,
						min => $rows->[0]->{start_int},
						max => $rows->[$#$rows]->{end_int},
						start_max => $rows->[$#$rows]->{start_int},
					};
				}
				else {
					$self->log->error('No tables');
					$ret->{error} = 'No tables';
					
				}
				$cv->end;
			});
		}
		
		# Get classes
		$query = "SELECT id, class FROM classes";
		$cv->begin;
		$self->log->trace($query);
		$db->{dbh}->query($query, sub {
			my ($dbh, $rows, $rv) = @_;
			
			if ($rv and $rows){
				$ret->{classes_by_id} = {};
				foreach my $row (@$rows){
					$ret->{classes_by_id}->{ $row->{id} } = $row->{class};
				}
			}
			else {
				$self->log->error('No classes');
				$ret->{error} = 'No classes';
			}
			$cv->end;
		});
		
		# Get fields
		$query = sprintf("SELECT DISTINCT field, class, field_type, input_validation, field_id, class_id, field_order,\n" .
			"IF(class!=\"\", CONCAT(class, \".\", field), field) AS fqdn_field, pattern_type\n" .
			"FROM %s.fields\n" .
			"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
			"JOIN %1\$s.classes t3 ON (t2.class_id=t3.id)\n", $db->{db});
		$cv->begin;
		$self->log->trace($query);
		my @fields;
		$db->{dbh}->query($query, sub {
			my ($dbh, $rows, $rv) = @_;
			
			if ($rv and $rows){
				foreach my $row (@$rows){
					push @fields, {
						fqdn_field => $row->{fqdn_field},
						class => $row->{class}, 
						value => $row->{field}, 
						text => uc($row->{field}),
						field_id => $row->{field_id},
						class_id => $row->{class_id},
						field_order => $row->{field_order},
						field_type => $row->{field_type},
						input_validation => $row->{input_validation},
						pattern_type => $row->{pattern_type},
					};
				}
			}
			else {
				$self->log->error('No fields');
				$ret->{error} = 'No fields';
			}
			$cv->end;
		});
	
		my $time_ranges = { indexes => {}, archive => {} };
		$ret->{totals} = {};
		foreach my $type (qw(indexes archive)){
			my $key = $type;
			if ($type eq 'archive'){
				$key = 'tables';
			}
			# Find min/max indexes
			my $min = 2**32;
			my $max = 0;
			my $start_max = 0;
			
			if (defined $ret->{$key}->{min} and $ret->{$key}->{min} < $min){
				$min = $ret->{$key}->{min};
			}
			if (defined $ret->{$key}->{max} and $ret->{$key}->{max} > $max){
				$max = $ret->{$key}->{max};
				$start_max = $ret->{$key}->{start_max};
			}
			if ($is_lite){
				$ret->{totals}->{$type} += $ret->{$key}->{records};
			}
			else {
				$ret->{totals}->{$type} += 0;
				foreach my $hash (@{ $ret->{$key}->{$key} }){
					next if $type eq 'archive' and not $hash->{table_type} eq 'archive';
					$ret->{totals}->{$type} += $hash->{records};
				}
			}
			
			if ($min == 2**32){
				$self->log->trace('No min/max found for type min');
				$min = 0;
			}
			if ($max == 0){
				$self->log->trace('No min/max found for type max');
				$max = $overall_start;
			}
			$ret->{$type . '_min'} = $min;
			$ret->{$type . '_max'} = $max;
			$ret->{$type . '_start_max'} = $start_max;
			$self->log->trace('Found min ' . $min . ', max ' . $max . ' for type ' . $type);
		}
		
		# Resolve class names into class_id's for excluded classes
		my $given_excluded_classes = $self->conf->get('excluded_classes') ? $self->conf->get('excluded_classes') : {};
		my $excluded_classes = {};
		foreach my $class_id (keys %{ $ret->{classes_by_id} }){
			if ($given_excluded_classes->{ lc($ret->{classes_by_id}->{$class_id}) } or
				$given_excluded_classes->{ uc($ret->{classes_by_id}->{$class_id}) }){
				$excluded_classes->{$class_id} = 1;
			}
		}
		
		# Find unique classes;
		$ret->{classes} = {};
		foreach my $class_id (keys %{ $ret->{classes_by_id} }){
			if ($excluded_classes->{$class_id}){
				delete $ret->{classes_by_id}->{$class_id};
				next;
			}
			$ret->{classes}->{ $ret->{classes_by_id}->{$class_id} } = $class_id;
		}
		
		# Find unique fields
		FIELD_LOOP: foreach my $field_hash (@fields){
			next if $excluded_classes->{ $field_hash->{class_id} };
			foreach my $already_have_hash (@{ $ret->{fields} }){
				if ($field_hash->{fqdn_field} eq $already_have_hash->{fqdn_field}){
					next FIELD_LOOP;
				}
			}
			push @{ $ret->{fields} }, $field_hash;
		}
		
		# Find unique field conversions
		$ret->{field_conversions} = {
			0 => {
				TIME => {
					0 => 'timestamp',
					100 => 'minute',
					101 => 'hour',
					102 => 'day',
				},
			},
		};
		foreach my $field_hash (@{ $ret->{fields} }){
			next if $excluded_classes->{ $field_hash->{class_id} };
			$ret->{field_conversions}->{ $field_hash->{class_id} } ||= {};
			if ($field_hash->{pattern_type} eq 'IPv4'){
				$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4} ||= {};
				$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4}->{ $field_hash->{field_order} } = $field_hash->{value};
			}
			elsif ($field_hash->{value} eq 'proto' and $field_hash->{pattern_type} eq 'QSTRING'){
				$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO} ||= {};
				$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO}->{ $field_hash->{field_order} } = $field_hash->{value};
			}
			elsif ($field_hash->{value} eq 'country_code' and $field_hash->{pattern_type} eq 'NUMBER'){
				$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE} ||= {};
				$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE}->{ $field_hash->{field_order} } = $field_hash->{value};
			}
		}
				
		# Find fields by arranged by order
		$ret->{fields_by_order} = {};
		foreach my $field_hash (@{ $ret->{fields} }){
			next if $excluded_classes->{ $field_hash->{class_id} };
			$ret->{fields_by_order}->{ $field_hash->{class_id} } ||= {};
			$ret->{fields_by_order}->{ $field_hash->{class_id} }->{ $field_hash->{field_order} } = $field_hash;
		}
		
		# Find fields by arranged by short field name
		$ret->{fields_by_name} = {};
		foreach my $field_hash (@{ $ret->{fields} }){
			next if $excluded_classes->{ $field_hash->{class_id} };
			$ret->{fields_by_name}->{ $field_hash->{value} } ||= [];
			push @{ $ret->{fields_by_name}->{ $field_hash->{value} } }, $field_hash;
		}
		
		# Find fields by type
		$ret->{fields_by_type} = {};
		foreach my $field_hash (@{ $ret->{fields} }){
			next if $excluded_classes->{ $field_hash->{class_id} };
			$ret->{fields_by_type}->{ $field_hash->{field_type} } ||= {};
			$ret->{fields_by_type}->{ $field_hash->{field_type} }->{ $field_hash->{value} } ||= [];
			push @{ $ret->{fields_by_type}->{ $field_hash->{field_type} }->{ $field_hash->{value} } }, $field_hash;
		}
		
		$ret->{directives} = $Fields::Reserved_fields;
		
		$ret->{updated_at} = time();
		$ret->{took} = (time() - $overall_start);
		
		if ($self->conf->get('version')){
			$ret->{version} = $self->conf->get('version');
		}
		
		$self->log->trace('get_info finished in ' . $ret->{took});
	
		$cv->end;
	});
}

sub _get_db {
	my $self = shift;
	my $cb = shift;
	my $conf = $self->conf->get('data_db');
	
	my $start = Time::HiRes::time();
	my $db = {};
	
	my $db_name = 'syslog';
	if ($conf->{db}){
		$db_name = $conf->{db};
	}
	
	my $mysql_port = 3306;
	if ($conf->{port}){
		$mysql_port = $conf->{port};
	}

	eval {
		$db = { db => $db_name };
		$db->{dbh} = SyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:database=' . $db_name . ';port=' . $mysql_port,  
			$conf->{username}, 
			$conf->{password}, 
			{
				mysql_connect_timeout => $self->db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
			}
		]);
	};
	if ($@){
		throw(502, $@, { mysql => 1 });
	}
	elsif ($DBI::errstr){
		throw(502, $DBI::errstr, { mysql => 1 });
	}
	
	$self->log->trace('All connected in ' . (Time::HiRes::time() - $start) . ' seconds');
	
	$cb->($db);
}

sub _peer_query {
	my ($self, $q, $cb) = @_;
	my ($query, $sth);
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		if (scalar keys %{ $q->peers->{given} }){
			if ($q->peers->{given}->{$peer}){
				# Normal case, fall through
			}
			elsif ($q->peers->{given}->{ $q->peer_label }){
				# Translate the peer label to localhost
				push @peers, '127.0.0.1';
				next;
			}
			else {
				# Node not explicitly given, skipping
				next;
			}
		}
		elsif (scalar keys %{ $q->peers->{excluded} }){
			next if $q->peers->{excluded}->{$peer};
		}
		push @peers, $peer;
	}
	
	# Make sure our localhost is first to avoid a read timeout with the way that the parallel async HTTP connections are queued.
	#  Otherwise, an initial connect is sent to each remote node, but no HTTP protocol information is sent until
	#  the localhost has completed its query since none of the localhost code uses anything async.
	#  This can violate the 5 second Plack read timeout if the local query takes longer than 5 seconds to execute.
	for (my $i = 0; $i < @peers; $i++){
		if ($peers[$i] eq '127.0.0.1' or $peers[$i] eq 'localhost'){
			my $tmp = $peers[0];
			$peers[0] = $peers[$i];
			$peers[$i] = $tmp;
			last;
		}
	}
	
	$self->log->trace('Executing global query on peers ' . join(', ', @peers));
	
	my %batches;
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {		
		if (scalar keys %batches){
			$query = 'INSERT INTO foreign_queries (qid, peer, foreign_qid) VALUES (?,?,?)';
			$sth = $self->db->prepare($query);
			foreach my $peer (sort keys %batches){
				$sth->execute($q->qid, $peer, $batches{$peer});
			}
			$self->log->trace('Updated query to have foreign_qids ' . Dumper(\%batches));
		}
		
		$cb->($q);
	});
	
	my $headers = { 'Content-type' => 'application/x-www-form-urlencoded', 'User-Agent' => $self->user_agent_name };
	foreach my $peer (@peers){
		my $peer_label = $peer;
		$cv->begin;
		
		if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
			if ($q->peer_label){
				$peer_label = $q->peer_label;
			}
			my $start = time();
			$self->local_query_preparsed($q, sub {
				my $ret_q = shift;
				unless ($ret_q and ref($ret_q) and $ret_q->can('stats')){
					if ($ret_q and ref($ret_q) eq 'Ouch'){
						throw($ret_q->code, $ret_q->message, $ret_q->data);
					}
					throw(500, 'Invalid query result');
				}
				
				if ($ret_q->groupby){
					$q->groupby($ret_q->groupby);
				}
				else {
					$q->clear_groupby();
				}
				
				if ($ret_q->results->records_returned and not $q->results->records_returned){
					$q->results($ret_q->results);
				}
				elsif ($ret_q->results->records_returned and $q ne $ret_q){
					$self->log->debug('query returned ' . $ret_q->results->records_returned . ' records, merging ' . Dumper($q->results) . ' with ' . Dumper($ret_q->results));
					$q->results->merge($ret_q->results, $q);
				}
				elsif ($ret_q->batch){
					my $current_message = $q->batch_message;
					$current_message .= $peer . ': ' . $ret_q->batch_message;
					$q->batch_message($current_message);
					$q->batch(1);
					#$batches{$peer} = $ret_q->qid;
					
					# Mark approximate if our peer results were
					if ($ret_q->results->is_approximate and not $q->results->results->is_approximate){
						$q->results->is_approximate($ret_q->results->is_approximate);
					}
				}
				
				my $stats = {};
				foreach my $key (keys %{ $ret_q->stats }){
					next if $key eq 'peers' and ($peer eq '127.0.0.1' or $peer eq 'localhost');
					$stats->{$key} = $ret_q->stats->{$key};
				}
				$stats->{total_request_time} = (time() - $start);
				$q->stats->{peers} ||= {};
				$q->stats->{peers}->{$peer} = { %$stats };
				$cv->end;
			});
			next;
		}
		
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/query';
		
		# Propagate some specific directives provided in prefs through to children via meta_params
		my $meta_params = $q->meta_params;
		if ($q->user and $q->user->preferences and $q->user->preferences->{tree}){
			my $prefs = $q->user->preferences->{tree}->{default_settings};
			foreach my $pref (qw(orderby_dir timeout default_or)){
				if (exists $prefs->{$pref}){
					$meta_params->{$pref} = $prefs->{$pref};
				}
			}
		}
		
		my $request_body = 'permissions=' . uri_escape($self->json->encode($q->user->permissions))
			. '&q=' . uri_escape($self->json->encode({ query_string => $q->query_string, query_meta_params => $meta_params }))
			. '&peer_label=' . $peer_label;
		$self->log->trace('Sending request to URL ' . $url . ' with body ' . $request_body);
		my $start = time();
		
		if ($peer_conf->{headers}){
			foreach my $header_name (keys %{ $peer_conf->{headers} }){
				$headers->{$header_name} = $peer_conf->{headers}->{$header_name};
			}
		}
		$headers->{Authorization} = $self->_get_auth_header($peer);
		$q->peer_requests->{$peer} = http_post $url, $request_body, headers => $headers, sub {
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
				$q->add_warning($e->code, $e->message, $e->data);
				delete $q->peer_requests->{$peer};
				$cv->end;
				return;
			}
			eval {
				my $raw_results = $self->json->decode($body);
				if ($raw_results and ref($raw_results) and $raw_results->{error}){
					$self->log->error('Peer ' . $peer_label . ' got error: ' . $raw_results->{error});
					$q->add_warning(502, 'Peer ' . $peer_label . ' encountered an error.', { http => $peer });
					return;
				}
				my $is_groupby = $raw_results->{groupby} and scalar @{ $raw_results->{groupby} } ? 1 : 0;
				my $results_package = $is_groupby ? 'Results::Groupby' : 'Results';
				my $results_obj = $results_package->new(results => $raw_results->{results}, 
					total_records => $raw_results->{totalRecords}, total_docs => $raw_results->{totalRecords}, is_approximate => $raw_results->{approximate});
				if ($results_obj->records_returned and not $q->results->records_returned){
					$q->results($results_obj);
				}
				elsif ($results_obj->records_returned){
					$self->log->debug('query returned ' . $results_obj->records_returned . ' records, merging ' . Dumper($q->results) . ' with ' . Dumper($results_obj));
					$q->results->merge($results_obj, $q);
				}
				elsif ($raw_results->{batch}){
					my $current_message = $q->batch_message;
					$current_message .= $peer . ': ' . $raw_results->{batch_message};
					$q->batch_message($current_message);
					$q->batch(1);
					$batches{$peer} = $raw_results->{qid};
				}
				
				# Mark approximate if our peer results were
				if ($results_obj->is_approximate and not $q->results->is_approximate){
					$q->results->is_approximate($results_obj->is_approximate);
				}
				
				if ($raw_results->{warnings} and ref($raw_results->{warnings}) eq 'ARRAY'){
					foreach my $warning (@{ $raw_results->{warnings} }){ 
						push @{ $q->warnings }, $warning;
					}
				}
				if ($is_groupby){
					$q->groupby($raw_results->{groupby}->[0]);
				}
				else {
					$q->clear_groupby();
				}
				my $stats = $raw_results->{stats};
				$stats ||= {};
				$stats->{total_request_time} = (time() - $start);
				$q->stats->{peers} ||= {};
				$q->stats->{peers}->{$peer} = { %$stats };
			};
			if ($@){
				$self->log->error($@ . 'url: ' . $url . "\nbody: " . $request_body);
				$q->add_warning(502, 'Invalid results back from peer ' . $peer_label, { http => $peer });
			}	
			delete $q->peer_requests->{$peer};
			$cv->end;
		};
	}
	$cv->end;
}

sub _get_auth_header {
	my $self = shift;
	my $peer = shift;
	
	my $timestamp = CORE::time();
	
	my $peer_conf = $self->conf->get('peers/' . $peer);
	die('no apikey or user found for peer ' . $peer) unless $peer_conf->{username} and $peer_conf->{apikey};
	return 'ApiKey ' . $peer_conf->{username} . ':' . $timestamp . ':' . sha512_hex($timestamp . $peer_conf->{apikey});
}

sub _check_auth_header {
	my $self = shift;
	my $req = shift;
	
	my ($username, $timestamp, $apikey);
	if ($req->header('Authorization')){
		($username, $timestamp, $apikey) = $req->header('Authorization') =~ /ApiKey ([^\:]+)\:([^\:]+)\:([^\s]+)/;
	}
	
	# Authenticate via apikey
	unless ($username and $timestamp and $apikey){
		$self->log->error('No apikey given');
		return 0;
	}
	unless ($timestamp > (CORE::time() - $Auth_timestamp_grace_period)
		and $timestamp <= (CORE::time() + $Auth_timestamp_grace_period)){
		$self->log->error('timestamp is out of date');
		return 0;
	}
	if ($self->conf->get('apikeys')->{$username} 
		and sha512_hex($timestamp . $self->conf->get('apikeys')->{$username}) eq $apikey){
		$self->log->trace('Authenticated ' . $username);
		return 1;
	}
	else {
		$self->log->error('Invalid apikey: '  . $username . ':' . $timestamp . ':' . $apikey);
		return 0;
	}
}

# Helper function to convert $@ into an Ouch exception if it isn't one already
#sub catch_any {
#	if ($@){
#		return blessed($@) ? $@ : new Ouch(500, $@);
#	}
#}

sub catch_any {
	my $self = shift;
	my $e = shift;
	return $e if blessed($e);
	$e =~ /(.+) at \S+ line \d+\.$/;
	$e = new Ouch(500, $1, {});
	$e->shortmess($1);
	return $e;
}

sub unicode_escape {

        my $str = shift;
        my $unicode_escaped_str = "";

	if (not $str) {
		return;
	}

        my $str_len = length($str);
        for (my $i = 0; $i <= $str_len; $i++) {
		my $char = substr($str, $i, 1);

		my $char_dec = ord($char);
		
		my $is_alphanumeric = ($char_dec >= 48 && $char_dec <= 57)
				   || ($char_dec >= 65 && $char_dec <= 90)
				   || ($char_dec >= 97 && $char_dec <= 122); 

		if (not $is_alphanumeric) {
		
			my $unicode_char_octets = Encode::encode("utf-16", 
								 $char);

			my $unicode_char_num_octets = length($unicode_char_octets);

			if ($unicode_char_num_octets == 4) {

				my $third_octet = substr($unicode_char_octets, 
							 2, 1);

				my $fourth_octet = substr($unicode_char_octets, 
							  3, 1);

				my $unicode_escaped_char = sprintf(
							"\\u%02x%02x", 
							ord($third_octet), 
							ord($fourth_octet));

				$unicode_escaped_str .= $unicode_escaped_char;

			}
		
		} else {

			$unicode_escaped_str .= $char;

		}

        }

        return $unicode_escaped_str;
}

1;
