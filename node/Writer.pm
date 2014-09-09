package Writer;
use Moose;
use Data::Dumper;
with 'MooseX::Traits';
with 'Log';
use DBI;

use Indexer;
use Reader;

our $Batch_limit = 0;
our $Livetail_batch_limit = 0;
our $Meta_update_ratio; # after how many batch inserts we check to see if we need a new table and index
our $Log;
our $Writer;

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => ( is => 'ro', isa => 'Config::JSON', required => 1 );
has 'db' => (is => 'rw', isa => 'DBI::db', required => 1);
has 'queue' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { queue_length => 'count' });
has 'rt' => (is => 'rw', isa => 'DBI::db');
has 'rt_query_template' => (is => 'rw', isa => 'Str');
has 'rt_sth' => (is => 'rw', isa => 'Object');
has 'index_query_template' => (is => 'rw', isa => 'Str');
has 'index_sth' => (is => 'rw', isa => 'Object');
has 'archive_query_template' => (is => 'rw', isa => 'Str');
has 'archive_sth' => (is => 'rw', isa => 'Object');
has 'current_id' => (traits => [qw(Counter)], is => 'rw', isa => 'Num', required => 1, default => 0, 
	handles => { inc_id => 'inc' });
has 'current_archive_id' => (traits => [qw(Counter)], is => 'rw', isa => 'Num', required => 1, default => 0, 
	handles => { inc_archive_id => 'inc' });	
has 'current_index' => (is => 'rw', isa => 'Str');
has 'current_table' => (is => 'rw', isa => 'Str');
has 'current_archive_table' => (is => 'rw', isa => 'Str');
has 'counter' => (traits => [qw(Counter)], is => 'rw', isa => 'Num', required => 1, default => 0,
	handles => { inc_counter => 'inc' });
has 'tempfile' => (is => 'rw', isa => 'Object');
has 'livetail_queue' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { livetail_queue_length => 'count' });

our $Livetail_query = 'INSERT INTO livetail_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
our $Livetail_sth;

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if ($params{config_file}){
		$params{conf} = Config::JSON->new($params{config_file});
	}
	
	$Log = $params{log}; # for use below
	
	if ($params{conf}){ # wrap this in a condition so that the right error message will be thrown if no conf
#		unless ($params{log}){
#			my $logdir = $params{conf}->get('logdir');
#			my $debug_level = $params{conf}->get('debug_level');
#			my $l4pconf = qq(
#				log4perl.category.ELSA       = $debug_level, File
#				log4perl.appender.File			 = Log::Log4perl::Appender::File
#				log4perl.appender.File.filename  = $logdir/node.log
#				log4perl.appender.File.syswrite = 1
#				log4perl.appender.File.recreate = 1
#				log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
#				log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
#				log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
#				log4perl.filter.ScreenLevel.LevelMin  = $debug_level
#				log4perl.filter.ScreenLevel.LevelMax  = ERROR
#				log4perl.filter.ScreenLevel.AcceptOnMatch = true
#				log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
#				log4perl.appender.Screen.Filter = ScreenLevel 
#				log4perl.appender.Screen.stderr  = 1
#				log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
#				log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
#			);
#			Log::Log4perl::init( \$l4pconf ) or die("Unable to init logger\n");
#			$params{log} = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");
#			$Log = $params{log}; # for use below
#		}
		my $dbh = DBI->connect(($params{conf}->get('database/dsn') or 'dbi:mysql:database=syslog;'), 
			$params{conf}->get('database/username'), 
			$params{conf}->get('database/password'), 
			{
				InactiveDestroy => 1,
				HandleError => \&_sql_error_handler,
				RaiseError => 1, 
				mysql_auto_reconnect => 1,
				mysql_local_infile => 1, # Needed by some MySQL implementations
			}
		) or die 'connection failed ' . $! . ' ' . $DBI::errstr;
		$params{db} = $dbh;
		
	}
	
	unless (-f $params{conf}->get('sphinx/config_file') ){
		my $indexer = new Indexer(log => $params{log}, conf => $params{conf});
		open(FH, '>' . $params{conf}->get('sphinx/config_file')) or die("Cannot open config file for writing: $!");
		print FH $indexer->get_sphinx_conf();
		close(FH);
		print 'Wrote new config to file ' .  $params{conf}->get('sphinx/config_file') . "\n";
	}
	
	if ($params{conf}->get('realtime')){
		$params{rt} = DBI->connect('dbi:mysql:host=' . $params{conf}->get('sphinx/host') . ';port=' . $params{conf}->get('sphinx/mysql_port'), 
				undef, undef, 
				{
					RaiseError => 1, 
					mysql_auto_reconnect => 1,
					mysql_multi_statements => 1,
					mysql_bind_type_guessing => 1,
					HandleError => \&_sql_error_handler,
					mysql_local_infile => 1,
				}
			) or die 'connection failed ' . $! . ' ' . $DBI::errstr;
		$Writer = '_realtime_write';
	}
	else {
		$Writer = '_file_write';
	}
	
	return \%params;
}

sub _sql_error_handler {
	my $errstr = shift;
	my $dbh = shift;
	my $query = $dbh->{Statement};
	my $full_errstr = 'SQL_ERROR: ' . $errstr . ', query: ' . $query; 
	$Log->error($full_errstr) unless $errstr =~ /livetail_results_ibfk_1/; # This error is expected when the tail has gone away
	return 1; # Stops RaiseError
	#die($full_errstr);
}


sub BUILD {
	my $self = shift;

	$Batch_limit = $self->conf->get('realtime/batch_limit');
	$Livetail_batch_limit = $self->conf->get('livetail/batch_limit') if $self->conf->get('livetail/batch_limit');
	$Meta_update_ratio = $self->conf->get('realtime/update_ratio') ? $self->conf->get('realtime/update_ratio') : 10;
	
	if ($Writer eq '_file_write'){
		$self->_set_temp_file();
		$self->_set_timer();
	}
	else {
		$self->_set_table_info();
	}
	
	# Prepare our insert query for later use
	$Livetail_sth = $self->db->prepare($Livetail_query);
	
	return $self;
}

sub _set_temp_file {
	my $self = shift;
	$self->tempfile(File::Temp->new( DIR => $self->conf->get('buffer_dir'), UNLINK => 0 ));
	unless ($self->tempfile){
		$self->log->error('Unable to create tempfile: ' . $!);
		return 0;
	}
	$self->tempfile->autoflush(1);
}

sub _set_table_info {
	my $self = shift;
	
	my $indexer = new Indexer(log => $self->log, conf => $self->conf);
	my $info = $indexer->get_current_index_info(); # creates new tables as necessary
	$self->current_id($info->{index}->{id});
	$self->log->debug('current_id: ' . $self->current_id . ', counter: ' . $self->counter);
	my $old_index = $self->current_index;
	$self->current_index($info->{realtime}->{table});
	$self->log->debug('old: ' . $old_index . ', new: ' . $self->current_index);
	$self->current_table($info->{index}->{table});
	$self->current_archive_id($info->{archive}->{id});
	$self->current_archive_table($info->{archive}->{table});
	my $intermediate_template = ' (id, timestamp, host_id, program_id, class_id, msg,';
	
	$self->rt_query_template('INSERT INTO ' . $info->{realtime}->{table} . $intermediate_template . ' attr_i0, attr_i1, attr_i2, attr_i3, attr_i4, attr_i5, attr_s0, attr_s1, attr_s2, attr_s3, attr_s4, attr_s5, day, hour, minute) VALUES ');
	$self->index_query_template('INSERT INTO ' . $info->{index}->{table} . $intermediate_template . ' i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5) VALUES ');
	$self->archive_query_template('INSERT INTO ' . $info->{archive}->{table} . $intermediate_template . ' i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5) VALUES ');
	
	if ($info->{realtime}->{do_init} or ($old_index and $old_index ne $self->current_index)){
		$self->_init_index($self->current_index, $self->current_table);
	}
	
	$self->update_directory();
	
	# Check to see if we need to roll the rt index
	if ($info->{realtime}->{records} >= $self->conf->get('sphinx/perm_index_size')){
		$self->_rollover();
	} 
}

sub _rollover {
	my $self = shift;
	
	# Send any pending in the queue
	$self->log->debug('batch insert from rollover');
	$self->realtime_batch_insert();
	
	# Fork our post-batch processor
	my $pid = fork();
	if ($pid){
		# Parent
		return 1;
	}
	# Child
	$self->log->trace('Child started');
	eval {
		my $indexer = new Indexer(log => $self->log, conf => $self->conf);
		$indexer->rotate_logs();
	};
	if ($@){
		$self->log->error('Child encountered error: ' . $@);
	}
	$self->log->trace('Child finished');
	exit;
}

sub _init_index {
	my $self = shift;
	my ($rt, $table) = @_;
	my ($query, $sth);
	$rt =~ /real_(\d+)$/;
	my $id = $1;
	$query = 'TRUNCATE RTINDEX ' . $rt;
	$self->rt->do($query);
	$self->log->trace('Truncated ' . $rt);
	
	$query = "REPLACE INTO indexes (id, start, end, first_id, last_id, table_id, type)\n" .
		"VALUES(?, ?, ?, ?, ?, (SELECT id FROM tables WHERE table_name=?), ?)";
	$sth = $self->db->prepare($query);
	$sth->execute($id, CORE::time(), CORE::time(), ($self->current_id + 1), ($self->current_id + 1), $table, 'realtime');
	$self->log->trace('Created new rt index ' . $rt . ' starting at id ' . ($self->current_id + 1) . ' and referring to table ' . $table);	
}

# Abstract our actual write method
sub write {
	shift->$Writer(shift);
}

sub _realtime_write {
	my $self = shift;
	my $line_arr = shift;
	$self->inc_counter;
	$self->inc_id;
	$self->inc_archive_id;
	unshift @$line_arr, $self->current_id;
	push @{ $self->queue }, $line_arr;
	if ($self->queue_length > $Batch_limit){
		#$self->log->debug('normal batch insert');
		$self->realtime_batch_insert();
		if ($self->counter % $Meta_update_ratio == 0){
			$self->_set_table_info();
		}
	}
}

sub _file_write {
	my $self = shift;
	my $line_arr = shift;
	unshift @$line_arr, 0;
	$self->tempfile->print(join("\t", @$line_arr) . "\n");
}

sub _set_timer {
	my $self = shift;
	local $SIG{ALRM} = sub {
		$self->log->trace("ALARM");
		my ($query, $sth);
		$query = 'INSERT INTO buffers (filename) VALUES (?)';
		$sth = $self->db->prepare($query);
		$sth->execute($self->tempfile->filename);
		$Log->trace('inserted filename ' . $self->tempfile->filename . ' with batch_counter ' . $self->counter);
		$self->counter(0);
		$self->_load_buffers();
		$self->_set_temp_file();
		$self->_set_timer();
	};
	alarm $self->conf->get('sphinx/index_interval');
	$self->log->trace('alarm set for interval ' . $self->conf->get('sphinx/index_interval'));
}

sub _load_buffers {
	my $self = shift;
	my $pid = fork();
	if ($pid){
		# Parent
		return 1;
	}
	# Child
	$Log->trace('Child started');
	eval {
		my $indexer = new Indexer(log => $self->log, conf => $self->conf);
		$indexer->load_buffers();
	};
	if ($@){
		$Log->error('Child encountered error: ' . $@);
	}
	$Log->trace('Child finished');
	exit; # done with child
}

sub realtime_batch_insert {
	my $self = shift;
	
	return unless $self->queue_length;
	
	my $query;
	my $placeholder_template = '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
	my $rt_placeholder_template = '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
	
	# Realtime Sphinx cannot have NULL's, so we need to replace those with zero or ''
	my $rt_queue = [];
	foreach (@{ $self->queue }){
		my $tmp_arr = [ @$_, int($_->[1]/86400), int($_->[1]/3600), int($_->[1]/60) ]; # add on day/hour/minute
		# + 1 because the id was already pushed onto the front of the array
		for (my $i = (Reader::FIELD_I0 + 1); $i <= (Reader::FIELD_I5 + 1); $i++){
			$tmp_arr->[$i] ||= 0;
		}
		for (my $i = (Reader::FIELD_S0 + 1); $i <= (Reader::FIELD_S5 + 1); $i++){
			$tmp_arr->[$i] ||= '';
		}
		push @$rt_queue, $tmp_arr;
	}
	
	$query = $self->rt_query_template . join(',', map { $rt_placeholder_template } @{ $self->queue });
	$self->rt_sth($self->rt->prepare($query));
	$self->rt_sth->execute(map { @$_ } @$rt_queue);
	
	$query = $self->index_query_template . join(',', map { $placeholder_template } @{ $self->queue });
	$self->index_sth($self->db->prepare($query));
	$self->index_sth->execute(map { @$_ } @{ $self->queue });
	
	$query = $self->archive_query_template . join(',', map { $placeholder_template } @{ $self->queue });
	$self->archive_sth($self->db->prepare($query));
	$self->archive_sth->execute(map { @$_ } @{ $self->queue });
	
	#$self->log->debug('cleared queue');
	$self->queue([]);
	
	$self->update_directory();
}

#sub livetail_write {
#	my $self = shift;
#	push @{ $self->livetail_queue }, shift;
#	if ($self->livetail_queue_length > $Livetail_batch_limit){
#		#$self->log->debug('normal batch insert');
#		$self->livetail_batch_insert();
#	}
#}
#
#sub livetail_batch_insert {
#	my $self = shift;
#	
#	return unless $self->livetail_queue_length;
#	
#	my $query = 'INSERT INTO livetail_results VALUES';
#	my $placeholder_template = '(?,?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?,?)';
#	
#	my @to_insert = map { @$_ } @{ $self->livetail_queue };
#	$query .= join(',', map { $placeholder_template } @{ $self->livetail_queue });
#	$Livetail_sth = $self->db->prepare($query);
#	$Livetail_sth->execute(map { @$_ } @{ $self->livetail_queue });
#	
#	$self->log->debug('cleared queue after insert ' . $Livetail_sth->rows . ' rows');
#	$self->livetail_queue([]);
#}

sub livetail_insert {
	my $self = shift;
	unless (defined $_[Reader::FIELD_CLASS_ID + 2] and defined $_[Reader::FIELD_MSG + 2]){
		$self->log->error('Received invalid line: ' . join(',', @_));
		return;
	}
	$_[2] = time(); # override time value (necessary if this is coming from reading a flat file and not live data)
	$Livetail_sth->execute(@_);
}

sub update_directory {
	my $self = shift;
	my ($query, $sth);
	
	$query = 'UPDATE tables SET end=NOW(), max_id=? WHERE table_name=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->current_id, $self->current_table);
	
	$query = 'UPDATE tables SET end=NOW(), max_id=? WHERE table_name=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->current_archive_id, $self->current_archive_table);
	
	$self->current_index =~ /real_(\d+)$/;
	$query = 'UPDATE indexes SET end=UNIX_TIMESTAMP(), last_id=IF(? > first_id, ?, last_id) WHERE type="realtime" AND id=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->current_id, $self->current_id, $1);
	#$self->log->debug('updated indexes and set id ' . $1 . ' to current_id ' . $self->current_id);
	return $sth->rows;
}

sub add_programs {
	my $self = shift;
	Indexer->new(log => $self->log, conf => $self->conf)->add_programs(shift);
}

__PACKAGE__->meta->make_immutable;
1;