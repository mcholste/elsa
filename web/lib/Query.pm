package Query;
use Moose;
with 'MooseX::Traits';
with 'Utils';
with 'Fields';
with 'Warnings';
with 'MooseX::Clone';
use Results;
use Time::HiRes;
use Data::Dumper;
use Search::QueryParser;
use Storable qw(dclone);
use Socket;
use Log::Log4perl::Level;
use Date::Manip;
use Try::Tiny;
use Ouch qw(:trytiny);;
use String::CRC32;
use Module::Pluggable sub_name => 'transform_plugins', require => 1, search_path => [ qw(Transform) ];
use CHI;

# Object for dealing with user queries

our $Default_limit = 100;
our $Tokenizer_regex = '[^A-Za-z0-9\\-\\.\\@\\_]';
our $Sql_tokenizer_regex = '[^-A-Za-z0-9\\.\\@\\_]';

# Required
has 'user' => (is => 'rw', isa => 'User', required => 1);
has 'parser' => (is => 'rw', isa => 'Object', required => 1);

# Required with defaults
has 'meta_params' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'type' => (is => 'rw', isa => 'Str', required => 1, default => 'index');
has 'results' => (is => 'rw', isa => 'Results', required => 1);
has 'start_time' => (is => 'ro', isa => 'Num', required => 1, default => sub { Time::HiRes::time() });
#has 'groupby' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
#	handles => { has_groupby => 'count', all_groupbys => 'elements', add_groupby => 'push' });
has 'groupby' => (is => 'rw', isa => 'Str', predicate => 'has_groupby');
has 'orderby' => (is => 'rw', isa => 'Str');
has 'orderby_dir' => (is => 'rw', isa => 'Str', required => 1, default => 'ASC');
has 'timeout' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'cancelled' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'archive' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'livetail' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'analytics' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'system' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'batch' => (is => 'rw', isa => 'Bool', required => 1, default => 0, trigger => \&_set_batch);
has 'limit' => (is => 'rw', isa => 'Int', required => 1, default => $Default_limit);
has 'offset' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'start' => (is => 'rw', isa => 'Int');
has 'end' => (is => 'rw', isa => 'Int');
has 'cutoff' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'transforms' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_transforms => 'count', all_transforms => 'elements', num_transforms => 'count', next_transform => 'shift' });
has 'connectors' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connectors => 'count', all_connectors => 'elements', num_connectors => 'count',
		connector_idx => 'get', add_connector => 'push' });
has 'connector_params' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connector_params => 'count', all_connector_params => 'elements', num_connector_params => 'count',
		connector_params_idx => 'get', add_connector_params => 'push' });
has 'terms' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'peers' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { { given => {}, excluded => {} } });
has 'hash' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'highlights' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'stats' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'timezone_difference' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { { start => 0, end => 0 } });
has 'peer_requests' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'id_ranges' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_id_ranges' => 'count', 'all_id_ranges' => 'elements' });
has 'max_query_time' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'use_sql_regex' => (is => 'rw', isa => 'Bool', required => 1, default => 1);
has 'original_timeout' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'datasources' => (is => 'rw', isa => 'HashRef');

# Optional
has 'query_string' => (is => 'rw', isa => 'Str');
has 'qid' => (is => 'rw', isa => 'Int');
has 'schedule_id' => (is => 'rw', isa => 'Int');
has 'raw_query' => (is => 'rw', isa => 'Str');
has 'comments' => (is => 'rw', isa => 'Str');
has 'time_taken' => (is => 'rw', isa => 'Num', trigger => \&_set_time_taken);
has 'batch_message' => (is => 'rw', isa => 'Str');
#has 'import_groupby' => (is => 'rw', isa => 'Str');
has 'peer_label' => (is => 'rw', isa => 'Str');
has 'from_peer' => (is => 'rw', isa => 'Str');
has 'estimated' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	$params{results} ||= new Results();
	$params{meta_info} = $params{parser}->meta_info;
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	my ($query, $sth);
	
	if ($self->has_transforms){
		foreach my $raw_transform ($self->all_transforms){
			$raw_transform =~ /(\w+)\(?([^\)]+)?\)?/;
			my $transform = lc($1);
			next if $transform eq 'subsearch';
			my $found = 0;
			foreach my $plugin ($self->transform_plugins()){
				if ($plugin =~ /\:\:$transform(?:\:\:|$)/i){
					$found = 1;
					last;
				}
			}
			throw(400, 'Transform ' . $transform . ' not found', { transform => $transform}) unless $found;
		}
	}
	
	# Override default limit if one is given in the config file
	if (defined $self->conf->get("default_results_limit")){
		$self->limit($self->conf->get("default_results_limit"));
	}
	# Override that with user preference
	if ($self->user->preferences and
		$self->user->preferences->{tree} and
		$self->user->preferences->{tree}->{default_settings} and
		defined $self->user->preferences->{tree}->{default_settings}->{limit}){
		$self->limit($self->user->preferences->{tree}->{default_settings}->{limit});
	}
	
	# Map directives to their properties
	foreach my $prop (keys %{ $self->parser->directives }){
		$self->$prop($self->parser->directives->{$prop});
	}
	
	unless (defined $self->query_string){
		# We may just be constructing this query as scaffolding for other things
		return $self;
	}
	
	$self->_normalize_terms();
	
	if ($self->groupby){
		$self->results( new Results::Groupby() );
	}
	
	if ($self->qid){
		$self->resolve_field_permissions($self->user); # finish this up from BUILDARGS now that we're blessed
		# Verify that this user owns this qid
		$query = 'SELECT qid FROM query_log WHERE qid=? AND uid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($self->qid, $self->user->uid);
		my $row = $sth->fetchrow_hashref;
		throw(403, 'User is not authorized for this qid', { user => $self->user->username }) unless $row;
		$self->log->level($ERROR) unless $self->conf->get('debug_all');
	}
	else {
		# Log the query
		$self->db->begin_work;
		$query = 'INSERT INTO query_log (uid, query, system, archive) VALUES (?, ?, ?, ?)';
		$sth = $self->db->prepare($query);
		$sth->execute( $self->user->uid, $self->raw_query ? $self->raw_query : $self->json->encode({ query_string => $self->query_string, query_meta_params => $self->meta_params }), 
			$self->system, 0 ); # set batch later
		$query = 'SELECT MAX(qid) AS qid FROM query_log';
		$sth   = $self->db->prepare($query);
		$sth->execute();
		my $row = $sth->fetchrow_hashref;
		$self->db->commit;
		
		$self->qid($row->{qid});

		$self->log->debug( "Received query with qid " . $self->qid . " at " . time() );
	}
	
	$self->hash($self->_get_hash($self->qid));
}

sub TO_JSON {
	my $self = shift;
	
	# Find highlights to inform the web client
	foreach my $boolean (qw(and or)){
		foreach my $key (sort keys %{ $self->terms->{$boolean} }){
			my @regex = $self->term_to_regex($self->terms->{$boolean}->{$key}->{value}, $self->terms->{$boolean}->{$key}->{field});
			foreach (@regex){
				$self->highlights->{$_} = 1 if defined $_;
			}
		}
	}
	
	my $ret = {
		qid => $self->qid,
		totalTime => $self->time_taken,
		results => $self->results->results, 
		totalRecords => ($self->results->total_records > $self->results->total_docs) ? $self->results->total_records : $self->results->total_docs,
		recordsReturned => $self->results->records_returned,	
		groupby => $self->groupby ? [ $self->groupby ] : [], # return an array for future faceting support
		orderby_dir => $self->orderby_dir,
		query_string => $self->query_string,
		query_meta_params => $self->meta_params,
		hash => $self->hash,
		highlights => $self->highlights,
		stats => $self->stats,
		approximate => $self->results->is_approximate,
		percentage_complete => $self->results->percentage_complete,
	};
	
	$ret->{query_meta_params}->{archive} = 1 if $self->archive;
	$ret->{query_meta_params}->{livetail} = 1 if $self->livetail;
	
	unless ($ret->{groupby} and ref($ret->{groupby}) and ref($ret->{groupby}) eq 'ARRAY' and scalar @{ $ret->{groupby} }){
		delete $ret->{groupby};
	}
	
	# Check to see if our result is bulky
	unless ($self->meta_params->{nobatch}){
		if ($self->results->is_bulk){
			$ret->{bulk_file} = $self->results->bulk_file;
			$ret->{batch_query} = $self->qid;
			my $link = sprintf('%sQuery/get_bulk_file?qid=%d', 
				$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost/',
				$self->qid);
			$ret->{batch_message} = 'Results: <a target="_blank" href="' . $link . '">' . $link . '</a>';
		}
		elsif ($self->batch_message){
			$ret->{batch_message} = $self->batch_message;
		}
		
		if ($self->batch){
			$ret->{batch} = 1;
		}
	}
	
	if ($self->has_warnings){
		$ret->{warnings} = $self->warnings;
	}
	
	return $ret;
}

sub _set_time_taken {
	my ( $self, $new_val, $old_val ) = @_;
	my ($query, $sth);
	
	# Update the db to ack
	$query = 'UPDATE query_log SET num_results=?, milliseconds=? '
	  		. 'WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute( $self->results->records_returned, $new_val, $self->qid );
	$self->log->trace('Set time taken for query ' . $self->qid . ' to ' . $new_val);
	
	return $sth->rows;
}

sub set_directive {
	my $self = shift;
	my $directive = shift;
	my $value = shift;
	my $op = shift;
	$op ||= '=';
	
	if ($directive eq 'start'){
		# special case for start/end
		if ($value =~ /^\d+$/){
			$self->start(int($value));
		}
		else {
			#$self->start(UnixDate(ParseDate($value), "%s"));
			my $tz_diff = $self->parser->timezone_diff($value);
			$self->start(UnixDate(ParseDate($value), "%s") + $tz_diff);
		}
		$self->log->debug('start is now: ' . $self->start .', ' . (scalar localtime($self->start)));
	}
	elsif ($directive eq 'end'){
		# special case for start/end
		if ($value =~ /^\d+$/){
			$self->end(int($value));
		}
		else {
			my $tz_diff = $self->parser->timezone_diff($value);
			$self->end(UnixDate(ParseDate($value), "%s") + $tz_diff);
		}
	}
	elsif ($directive eq 'limit'){
		# special case for limit
		$self->limit(sprintf("%d", $value));
		throw(400, 'Invalid limit', { term => 'limit' }) unless $self->limit > -1;
	}
	elsif ($directive eq 'offset'){
		# special case for offset
		$self->offset(sprintf("%d", $value));
		throw(400, 'Invalid offset', { term => 'offset' }) unless $self->offset > -1;
	}
	elsif ($directive eq 'groupby'){
		my $value = lc($value);
		#TODO implement groupby import with new import system
		my $field_infos = $self->get_field($value);
		$self->log->trace('$field_infos ' . Dumper($field_infos));
		if ($field_infos or $value eq 'node'){
			$self->groupby(lc($value));
			$self->log->trace("Set groupby " . Dumper($self->groupby));
		}
	}
	elsif ($directive eq 'orderby'){
		my $value = lc($value);
		my $field_infos = $self->get_field($value);
		$self->log->trace('$field_infos ' . Dumper($field_infos));
		if ($field_infos or $value eq 'node'){
			$self->orderby($value);
			$self->log->trace("Set orderby " . Dumper($self->orderby));
		}
	}
	elsif ($directive eq 'orderby_dir'){
		if (uc($value) eq 'DESC'){
			$self->orderby_dir('DESC');
		}
	}
	elsif ($directive eq 'node'){
		if ($value =~ /^[\w\.\:]+$/){
			if ($op eq '-'){
				$self->peers->{excluded}->{ $value } = 1;
			}
			else {
				$self->peers->{given}->{ $value } = 1;
			}
		}
	}
	elsif ($directive eq 'cutoff'){
		$self->limit($self->cutoff(sprintf("%d", $value)));
		throw(400, 'Invalid cutoff', { term => 'cutoff' }) unless $self->cutoff > -1;
		$self->log->trace("Set cutoff " . $self->cutoff);
	}
	elsif ($directive eq 'datasource'){
		delete $self->datasources->{sphinx}; # no longer using our normal datasource
		$self->datasources->{ $value } = 1;
		$self->log->trace("Set datasources " . Dumper($self->datasources));
	}
	elsif ($directive eq 'nobatch'){
		$self->meta_params->{nobatch} = 1;
		$self->log->trace("Set batch override.");
	}
	elsif ($directive eq 'livetail'){
		$self->meta_params->{livetail} = 1;
		$self->livetail(1);
		$self->archive(1);
		$self->log->trace("Set livetail.");
	}
	elsif ($directive eq 'archive'){
		$self->meta_params->{archive} = 1;
		$self->archive(1);
		$self->log->trace("Set archive.");
		next;
	}
	elsif ($directive eq 'analytics'){
		$self->meta_params->{analytics} = 1;
		$self->analytics(1);
		$self->log->trace("Set analytics.");
	}
	else {
		throw(400, 'Invalid directive', { term => $directive });
	}
}

sub cancel {
	my $self = shift;
	
	my ($query, $sth);
	$query = 'UPDATE query_log SET num_results=-2 WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->qid);
	return 1;
}

sub check_cancelled {
	my $self = shift;
	my ($query, $sth);
	$query = 'SELECT num_results FROM query_log WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->qid);
	my $row = $sth->fetchrow_hashref;
	if (defined $row->{num_results} and $row->{num_results} eq -2){
		$self->log->info('Query ' . $self->qid . ' has been cancelled');
		$self->cancelled(1);
		return 1;
	}
	return 0;
}

sub mark_batch_start {
	my $self = shift;
	# Record that we're starting so no one else starts it
	my ($query, $sth);
	$sth = $self->db->prepare('UPDATE query_log SET num_results=-1, archive=? WHERE qid=?');
	$sth->execute($$, $self->qid);
	return $sth->rows;
}

sub convert_to_archive {
	my $self = shift;
	$self->query_term_count(0);
	foreach my $boolean (qw(and or not)){
		foreach my $term (keys %{ $self->terms->{any_field_terms}->{$boolean} }){ 
			# Drop the term
			delete $self->terms->{any_field_terms}->{$boolean}->{$term};
			my $sphinx_term = $term;
			# Make sphinx term SQL term
			if ($sphinx_term =~ /^\(\@(class|host|program) (\d+)\)$/){
				$self->terms->{attr_terms}->{$boolean}->{'='}->{0}->{ $Fields::Field_order_to_meta_attr->{ $Fields::Field_to_order->{$1} } } = $2;
			}
			else {
				$self->terms->{any_field_terms_sql}->{$boolean}->{$term} = $sphinx_term;
			}
		}
	}
	# Put the field_terms_sql back to field_terms now that we've done the count
	if ($self->terms->{field_terms_sql}){
		foreach my $boolean (keys %{ $self->terms->{field_terms_sql} }){
			foreach my $class_id (keys %{ $self->terms->{field_terms_sql}->{$boolean} }){
				foreach my $raw_field (keys %{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id} }){
					foreach my $term (@{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id}->{$raw_field} }){
						push @{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field} }, $term;
					}
				}
			}
		}
	}
	delete $self->terms->{field_terms_sql};
}

sub dedupe_warnings {
	my $self = shift;
	my %uniq;
	foreach my $warning ($self->all_warnings){
		my $key;
		if (blessed($warning)){
			$key = $warning->code . $warning->message . $self->json->encode($warning->data);
		}
		elsif (ref($warning)){
			$key = $warning->{code} . $warning->{message} . $self->json->encode($warning->{data});
		}
		else {
			$self->log->warn('Improperly formatted warning received: ' . $warning);
			$key = $warning;
			$warning = { code => 500, message => $warning, data => {} };
		}
		$uniq{$key} ||= [];
		push @{ $uniq{$key} }, $warning;
	}
	
	my @dedupe;
	foreach my $key (keys %uniq){
		# Remove 416's if we have data
		if ($uniq{$key}->[0]->{code} eq 416 and $self->results->records_returned){
			next;
		}
		push @dedupe, $uniq{$key}->[0];
	}
	
	
		
	$self->warnings([@dedupe]);
}

sub has_stopword_terms {
	my $self = shift;
	my $terms_count = (scalar keys %{ $self->terms->{any_field_terms_sql}->{and} }) + (scalar keys %{ $self->terms->{any_field_terms_sql}->{not} });
	if ($self->terms->{field_terms_sql}){
		foreach my $boolean (keys %{ $self->terms->{field_terms_sql} }){
			foreach my $class_id (keys %{ $self->terms->{field_terms_sql}->{$boolean} }){
				foreach my $raw_field (keys %{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id} }){
					foreach my $term (@{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id}->{$raw_field} }){
						$terms_count++;
					}
				}
			}
		}
	}
	return $terms_count;
}

sub count_terms {
	my $self = shift;
	my $given_boolean = shift;
	my $count = 0;
	foreach my $boolean (keys %{ $self->terms }){
		next if $given_boolean and $boolean ne $given_boolean;
		$count += scalar keys %{ $self->terms->{$boolean} };
	}
	return $count;
}

sub _normalize_terms {
	my $self = shift;
	return 1;
}

sub _build_query {
	my $self = shift;
}

sub _get_permissions_clause {
	my $self = shift;
}

sub permitted_classes {
	my $self = shift;
	if ($self->user->permissions->{class_id}->{0}){
		my %classes = %{ $self->meta_info->{classes_by_id} };
		delete $classes{0};
		return \%classes;
	}
	else {
		my %ret;
		foreach my $class_id (keys %{ $self->meta_info->{classes_by_id} }){
			next unless $class_id; # skip 0
			if ($self->user->is_permitted('class_id', $class_id)){
				$ret{$class_id} = $self->meta_info->{classes_by_id}->{$class_id};
			}
		}
		return \%ret;
	}
}


sub execute {
	my $self = shift;
	my $cb = shift;
	$cb->();
}

sub execute_batch {
	my $self = shift;
	my $cb = shift;
	
	$self->batch(1); # trigger updates MySQL to set archive=1
	
	$cb->();
}

sub _set_batch {
	my ( $self, $new_val, $old_val ) = @_;
	my ($query, $sth);
	$query = 'UPDATE query_log SET archive=? WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($new_val, $self->qid);
	return $sth->rows;
}

sub transform_results {
	my $self = shift;
	my $cb = shift;

	$self->log->debug('transforms: ' . Dumper($self->transforms));
	
	if (not $self->has_transforms){
		$cb->($self->results);
		return;
	}
	
	$self->log->debug('started with transforms: ' . Dumper($self->transforms));
	my $raw_transform = $self->next_transform;
	$self->log->debug('ended with transforms: ' . Dumper($self->transforms));
	chop($raw_transform) if $raw_transform =~ /\)$/;
	#$raw_transform =~ /(\w+)\(?([^\)]+)?\)?/;
	$raw_transform =~ /(\w+)\(?(.*)/;
	my $transform = lc($1);
	my @transform_args = $2 ? split(/\,/, $2) : ();
	# Remove any args which are all whitespace
	for (my $i = 0; $i < @transform_args; $i++){
		if ($transform_args[$i] =~ /^\s+$/){
			splice(@transform_args, $i, 1);
		}
	}
	
	if ($transform eq 'subsearch'){
		$self->_subsearch(\@transform_args, sub {
			my $q = shift;
			unless ($q){
				$self->log->debug('No query object to tranform');
				$cb->();
				return;
			}
			$self->log->debug('got subsearch results: ' . Dumper($q->TO_JSON));
			$self->results($q->results);
			foreach my $warning ($q->all_warnings){
				$self->add_warning($warning);
			}
			if ($q->groupby){
				$self->groupby($q->groupby);
			}
			else {
				$self->groupby('');
			}
			$q->transform_results($cb);
		});
	}
	else {
		my $num_found = 0;
		my $cache = CHI->new(driver => 'RawMemory', datastore => {});
		foreach my $plugin ($self->transform_plugins()){
			if ($plugin =~ /\:\:$transform(?:\:\:|$)/i){
				$self->log->debug('loading plugin ' . $plugin);
				try {
					my $plugin_object = $plugin->new(
						query_string => $self->query_string,
						query_meta_params => $self->meta_params,
						conf => $self->conf,
						log => $self->log,
						user => $self->user,
						cache => $cache,
						results => $self->results,
						args => [ @transform_args ],
						on_transform => sub {
							my $results_obj = shift;
							if ($results_obj->can('groupby')){
								$self->groupby( ($results_obj->all_groupbys )[0] );
							}
							$self->results($results_obj);
							$self->_post_transform();
							$self->log->debug('Finished transform ' . $plugin . ' with results ' . Dumper($self->results->results));
							$self->transform_results($cb);
						},
						on_error => sub {
							$self->log->error('Error creating plugin ' . $plugin . ' with data ' 
								. Dumper($self->results->results) . ' and args ' . Dumper(\@transform_args) . ': ' . $@);
							$self->add_warning(500, $@, { transform => $self->peer_label });
							$self->transform_results($cb);
						});
						$num_found++;
				}
				catch {
					my $e = shift;
					$self->log->error('Error creating plugin ' . $plugin . ' with data ' 
						. Dumper($self->results->results) . ' and args ' . Dumper(\@transform_args) . ': ' . $e);
					$self->add_warning(500, $e, { transform => $self->peer_label });
				};
				last;
			}
		}
		unless ($num_found){
			my $err = "failed to find transform $transform, only have transforms " .
				join(', ', $self->transform_plugins());
			$self->log->error($err);
			$self->add_warning(400, $err, { transform => $self->peer_label });
			$self->transform_results($cb);
		}
	}
}

sub _post_transform {
	my $self = shift;
	
	# Now go back and insert the transforms
	if ($self->groupby){
		my @groupby_results;
		foreach my $row ($self->results->all_results){
			$self->log->debug('row: ' . Dumper($row));
			if (exists $row->{transforms} and scalar keys %{ $row->{transforms} }){
				foreach my $transform (sort keys %{ $row->{transforms} }){
					next unless ref($row->{transforms}->{$transform}) eq 'HASH';
					foreach my $field (sort keys %{ $row->{transforms}->{$transform} }){
						if (ref($row->{transforms}->{$transform}->{$field}) eq 'HASH'){
							my $add_on_str = $row->{_groupby};
							foreach my $data_attr (keys %{ $row->{transforms}->{$transform}->{$field} }){
								if (ref($row->{transforms}->{$transform}->{$field}->{$data_attr}) eq 'ARRAY'){
									$add_on_str .= ' ' . $data_attr . '=' . join(',', @{ $row->{transforms}->{$transform}->{$field}->{$data_attr} }); 
								}
								else {
									$add_on_str .= ' ' . $data_attr . '=' .  $row->{transforms}->{$transform}->{$field}->{$data_attr};
								}
							}
							$self->log->debug('add_on_str: ' . $add_on_str);
							$row->{_groupby} = ($row->{ $self->groupby } . ' ' . $add_on_str);
						}
						# If it's an array, we want to concatenate all fields together.
						elsif (ref($row->{transforms}->{$transform}->{$field}) eq 'ARRAY'){
							my $arr_add_on_str = '';
							foreach my $value (@{ $row->{transforms}->{$transform}->{$field} }){
								$arr_add_on_str .= ' ' . $field . '=' .  $value;
							}
							if ($arr_add_on_str ne ''){
								$row->{_groupby} = ($row->{ $self->groupby } . ' ' . $arr_add_on_str);
							}
						}
					}
				}
				push @groupby_results, $row;
			}
			else {
				push @groupby_results, $row;
			}
		}
		$self->log->debug('@groupby_results: ' . Dumper(\@groupby_results));
		$self->results(Results::Groupby->new(results => { $self->groupby => [ @groupby_results ] }));		
	}
	else {
		my @final;
		#$self->log->debug('$transform_args->{results}: ' . Dumper($transform_args->{results}));
		#$self->log->debug('results: ' . Dumper($q->results->results));
		foreach my $row ($self->results->all_results){
			foreach my $transform (sort keys %{ $row->{transforms} }){
				next unless ref($row->{transforms}->{$transform}) eq 'HASH';
				foreach my $transform_field (sort keys %{ $row->{transforms}->{$transform} }){
					if ($transform_field eq '__REPLACE__'){
						foreach my $transform_key (sort keys %{ $row->{transforms}->{$transform}->{$transform_field} }){
							my $value = $row->{$transform_key};
							# Perform replacement on fields
							foreach my $row_field_hash (@{ $row->{_fields} }){
								if ($row_field_hash->{field} eq $transform_key){
									$row_field_hash->{value} = $value;
								}
							}
							# Perform replacement on attrs
							foreach my $row_key (keys %{ $row }){
								next if ref($row->{$row_key});
								if ($row_key eq $transform_key){
									$row->{$row_key} = $value;
								}
							}
						}
					}
					elsif (ref($row->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
						foreach my $transform_key (sort keys %{ $row->{transforms}->{$transform}->{$transform_field} }){
							my $value = $row->{transforms}->{$transform}->{$transform_field}->{$transform_key};
							if (ref($value) eq 'ARRAY'){
								foreach my $value_str (@$value){
									push @{ $row->{_fields} }, { 
										field => $transform_field . '.' . $transform_key, 
										value => $value_str, 
										class => 'Transform.' . $transform,
									};
								}
							}
							else {			
								push @{ $row->{_fields} }, { 
									field => $transform_field . '.' . $transform_key, 
									value => $value,
									class => 'Transform.' . $transform,
								};
							}
						}
					}
					elsif (ref($row->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY'){
						foreach my $value (@{ $row->{transforms}->{$transform}->{$transform_field} }){
							push @{ $row->{_fields} }, { 
								field => $transform . '.' . $transform_field, 
								value => $value,
								class => 'Transform.' . $transform,
							};
						}
					}
				}
			}
			push @final, $row;
		}
		$self->log->debug('final: ' . Dumper(\@final));
		$self->results(Results->new(results => [ @final ]));
		$self->groupby('');
	}
}

sub _subsearch {
	my $self = shift;
	my $args = shift;
	my $cb = shift;
	
	if (not $self->groupby){
		throw(400, 'Subsearch requires preceding query to be a groupby', { term => 'subsearch' });
	}
	
	$self->log->debug('args: ' . Dumper($args));
	
	my $subsearch_query_string = shift @$args;
	my $subsearch_field = shift @$args;
	
	#$self->log->debug('all terms: ' . Dumper($self->results->all_results));
	
	# Get the unique values from our current results
	my @terms;
	foreach my $record ($self->results->all_results){
		my $term = $record->{_groupby};
		if ($subsearch_field){
			push @terms, $subsearch_field . ':' . $term;
		}
		else {
			push @terms, $term;
		}
	}
	
	$subsearch_query_string .= ' ' . join(' OR ', @terms);
	$self->log->trace('Subsearch query: ' . $subsearch_query_string);
	my $qp = QueryParser->new(conf => $self->conf, log => $self->log, meta_info => $self->parser->meta_info, 
		query_string => $subsearch_query_string, transforms => $self->transforms);
	my $q;
	
	try {
		$q = $qp->parse();
		
		# Now that we've parsed the query, we can set groupby accordingly
		if ($q->groupby){
			$self->groupby($q->groupby);
		}
		else {
			$self->groupby('');
		}
		
		$q->start($self->start) if $self->start;
		$q->end($self->end) if $self->end;
	}
	catch {
		my $e = shift;
		$self->log->error('Error parsing subquery: ' . $e);
		throw(400, 'Failed to parse subsearch query', { query_string => join(' ', @$args) });
	};
	
	if ($self->results->records_returned){
		$q->execute(sub {
			$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
			$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000));
		
			# Apply transforms
			$q->transform_results(sub { 
				$q->dedupe_warnings();
				$cb->($q);
			});
		});
	}
	else {
		$self->log->info(sprintf("Query " . $q->qid . " did not run due to lack of results in preceding search"));
		$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000));
	
		# Apply transforms
		$q->transform_results(sub { 
			$q->dedupe_warnings();
			$cb->($q);
		});
	}
}

sub estimate_query_time {
	my $self = shift;
	return $self->estimated;
}

sub _classes_for_field {
	my $self = shift;
	my $field_name = shift;
	
	my $field_hashes = $self->get_field($field_name);
	unless ($field_hashes){
		return $self->permitted_classes;
	}
	my %classes;
	foreach my $class_id (keys %$field_hashes){
		$classes{$class_id} = $self->meta_info->{classes_by_id}->{$class_id};
	}
	return \%classes;
}

sub _value {
	my $self = shift;
	my $hash = shift;
	my $class_id = shift;
	
	my $attr = $self->_attr($hash->{field}, $class_id);
	
	my $orig_value = $hash->{value};
	$hash->{value} =~ s/^\"//;
	$hash->{value} =~ s/\"$//;
	
	$self->log->trace('$hash: ' . Dumper($hash) . ' value: ' . $hash->{value} . ' $attr: ' . $attr);
	
	unless (defined $class_id and defined $hash->{value} and defined $attr){
		$self->log->error('Missing an arg: ' . $class_id . ', ' . $hash->{value} . ', ' . $attr);
		return $hash->{value};
	}
	
	if ($attr eq 'host_id'){ #host is handled specially
		my @ret;
		if ($hash->{value} =~ /^"?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"?$/) {
			@ret = ( unpack('N*', inet_aton($1)) ); 
		}
		elsif ($hash->{value} =~ /^"?([a-zA-Z0-9\-\.]+)"?$/){
			my $host_to_resolve = $1;
			unless ($host_to_resolve =~ /\./){
				my $fqdn_hostname = Sys::Hostname::FQDN::fqdn();
				$fqdn_hostname =~ /^[^\.]+\.(.+)/;
				my $domain = $1;
				$self->log->debug('non-fqdn given, assuming to be domain: ' . $domain);
				$host_to_resolve .= '.' . $domain;
			}
			$self->log->debug('resolving and converting host ' . $host_to_resolve. ' to inet_aton');
			my $res   = Net::DNS::Resolver->new;
			my $query = $res->search($host_to_resolve);
			if ($query){
				my @ips;
				foreach my $rr ($query->answer){
					next unless $rr->type eq "A";
					$self->log->debug('resolved host ' . $host_to_resolve . ' to ' . $rr->address);
					push @ips, $rr->address;
				}
				if (scalar @ips){
					foreach my $ip (@ips){
						my $ip_int = unpack('N*', inet_aton($ip));
						push @ret, $ip_int;
					}
				}
				else {
					throw(500, 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring, { external_dns => $host_to_resolve });
				}
			}
			else {
				throw(500, 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring, { external_dns => $host_to_resolve });
			}
		}
		else {
			throw(400, 'Invalid host given: ' . Dumper($hash->{value}), { host => $hash->{value} });
		}
		if (wantarray){
			return @ret;
		}
		else {
			return $ret[0];
		}
	}
	elsif ($attr eq 'class_id'){
		return $self->meta_info->{classes}->{ uc($hash->{value}) };
	}
	elsif ($attr eq 'program_id'){
		$self->log->trace("Converting $hash->{value} to program_id");
		return crc32(lc($hash->{value}));
	}
	elsif ($attr =~ /^attr_s\d+$/){
		# String attributes need to be crc'd
		$self->log->debug('computed crc32 value ' . crc32($hash->{value}) . ' for value ' . $hash->{value});
		return crc32($hash->{value});
	}
	else {
		my $field_order;
		foreach (keys %{ $Fields::Field_order_to_attr }){
			if ($Fields::Field_order_to_attr->{$_} eq $attr){
				$field_order = $_;
			}
		}
		if (defined $field_order){
			if ($self->meta_info->{field_conversions}->{ $class_id }->{'IPv4'}
				and $self->meta_info->{field_conversions}->{ $class_id }->{'IPv4'}->{$field_order}
				and $hash->{value} =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
				$self->log->debug('converting ' . $hash->{value} . ' to IPv4 value ' . unpack('N', inet_aton($hash->{value})));
				return unpack('N', inet_aton($hash->{value}));
			}
			elsif ($self->meta_info->{field_conversions}->{ $class_id }->{PROTO} 
				and $self->meta_info->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
				$self->log->trace("Converting $hash->{value} to proto");
				return exists $Fields::Proto_map->{ uc($hash->{value}) } ? $Fields::Proto_map->{ uc($hash->{value}) } : int($hash->{value});
			}
			elsif ($self->meta_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
				and $self->meta_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
				if ($Fields::Field_order_to_attr->{$field_order} =~ /attr_s/){
					$self->log->trace("Converting $hash->{value} to CRC of country_code");
					return crc32(join('', unpack('c*', pack('A*', uc($hash->{value})))));
				}
				else {
					$self->log->trace("Converting $hash->{value} to country_code");
					return join('', unpack('c*', pack('A*', uc($hash->{value}))));
				}
			}
			else {
				return $hash->{value};
			}
		}
		else {
			# Integer value
			if ($orig_value == 0 or int($orig_value)){
				return $orig_value;
			}
			else {
				# Try to find an int and use that
				$orig_value =~ s/\\?\s//g;
				if (int($orig_value)){
					return $orig_value;
				}
				else {
					throw(400, 'Invalid query term, not an integer: ' . $orig_value, { term => $orig_value });
				}
			}
		}
	}
	throw(500, 'Unable to find value for field ' . $hash->{field}, { term => $hash->{field} });
}

sub _attr {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	if (defined $Fields::Field_to_order->{$field_name}){
		return $Fields::Field_order_to_attr->{ $Fields::Field_to_order->{$field_name} };
	}
	
	my $field_hashes = $self->get_field($field_name);
	if ($field_hashes->{$class_id}){
		return $Fields::Field_order_to_attr->{ $field_hashes->{$class_id}->{field_order} };
	}
	return;
}

sub term_to_regex {
	my $self = shift;
	my $term = shift;
	my $field_name = shift;
	my $regex = $term;
	return if $field_name and $field_name eq 'class'; # we dont' want to highlight class integers
	if (my @m = $regex =~ /^\(+ (\@\w+)\ ([^|]+)? (?:[\|\s]? ([^\)]+))* \)+$/x){
		if ($m[0] eq '@class'){
			return; # we dont' want to highlight class integers
		}
		else {
			my @ret = @m[1..$#m];# don't return the field name
			foreach (@ret){
				$_ = '(?:^|' . $Tokenizer_regex . ')(' . $_ . ')(?:' . $Tokenizer_regex . '|$)';
			}
			return  @ret;
		}
	}
	elsif (@m = $regex =~ /^\( ([^|]+)? (?:[\|\s]? ([^\)]+))* \)+$/x){
		foreach (@m){
			$_ = '(?:^|' . $Tokenizer_regex . ')(' . $_ . ')(?:' . $Tokenizer_regex . '|$)';
		}
		return @m;
	}
	$regex =~ s/^\s{2,}/\ /;
	$regex =~ s/\s{2,}$/\ /;
	$regex =~ s/\s/\./g;
	$regex =~ s/\\{2,}/\\/g;
	$regex =~ s/[^a-zA-Z0-9\.\_\-\@]//g;
	$regex = '(?:^|' . $Tokenizer_regex . ')(' . $regex . ')(?:' . $Tokenizer_regex . '|$)';
	return ($regex);
}


sub _is_int_field {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	if ($self->_is_meta($field_name, $class_id)){
		return 1;
	}
	
	
	my $field_hashes = $self->get_field($field_name);
	if ($field_hashes->{$class_id} and $field_hashes->{$class_id}->{field_type} eq 'int'){
		return 1;
	}
	
	return;
}

sub _is_meta {
	my $self = shift;
	my $field_or_attr = shift;
	
	if (defined $Fields::Field_to_order->{$field_or_attr} 
		and $Fields::Field_order_to_meta_attr->{ $Fields::Field_to_order->{$field_or_attr} }){
		return 1;
	}
	else {
		for (values %$Fields::Field_order_to_meta_attr){
			if ($_ eq $field_or_attr){
				return 1;
			}
		}
	}
	return 0;
}

sub _search_field {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	my $field_hashes = $self->get_field($field_name);
	if ($field_hashes->{$class_id}){
		return $Fields::Field_order_to_field->{ $field_hashes->{$class_id}->{field_order} };
	}
	elsif ($field_hashes->{0}){
		return $Fields::Field_order_to_field->{ $field_hashes->{0}->{field_order} };
	}
		
	return;
}

sub _term_to_sql_term {
	my $self = shift;
	my $term = shift;
	my $field_name = shift;
	
	my $regex = $term;
	return if $field_name and $field_name eq 'class'; # we dont' want to highlight class integers
	if (my @m = $regex =~ /^\(+ (\@\w+)\ ([^|]+)? (?:[\|\s]? ([^\)]+))* \)+$/x){
		if ($m[0] eq '@class'){
			return; # we dont' want to search this
		}
		else {
			my @ret = @m[1..$#m];# don't return the field name
			foreach (@ret){
				$_ = '(^|' . $Sql_tokenizer_regex . ')(' . $_ . ')(' . $Sql_tokenizer_regex . '|$)';
			}
			return $ret[0];
		}
	}
	elsif (@m = $regex =~ /^\( ([^|]+)? (?:[\|\s]? ([^\)]+))* \)+$/x){
		foreach (@m){
			$_ = '(^|' . $Sql_tokenizer_regex . ')(' . $_ . ')(' . $Sql_tokenizer_regex . '|$)';
		}
		return $m[0];
	}
	$regex =~ s/^\s{2,}/\ /;
	$regex =~ s/\s{2,}$/\ /;
	$regex =~ s/\s/\./g;
	$regex =~ s/\\{2,}/\\/g;
	$regex =~ s/[^a-zA-Z0-9\.\_\-\@]//g;
	$regex = '(^|' . $Sql_tokenizer_regex . ')(' . $regex . ')(' . $Sql_tokenizer_regex . '|$)';
	return $regex;
}

sub _find_import_ranges {
	my $self = shift;
	
	my $start = time();
	my ($query, $sth);
	my @id_ranges;
	
	my $db_name = $self->conf->get('data_db/db') ? $self->conf->get('data_db/db') : 'syslog';	
		
	# Handle dates specially
	my %date_terms;
	foreach my $term_hash ($self->parser->all_import_search_terms){
		next unless $term_hash->{field} eq 'date';
		$date_terms{ $term_hash->{boolean} } ||= [];
		push @{ $date_terms{ $term_hash->{boolean} } }, $term_hash;
	}
	
	if (scalar keys %date_terms){
		$query = 'SELECT * from ' . $db_name . '.imports WHERE NOT ISNULL(first_id) AND NOT ISNULL(last_id) AND ';
		my @clauses;
		my @terms;
		my @values;
		foreach my $term_hash (@{ $date_terms{and} }){
			my $epoch = UnixDate(ParseDate($term_hash->{value}), '%s');
			$epoch = $epoch + $self->parser->timezone_diff($epoch);	
			throw(400, 'Invalid import date', { term => $term_hash->{value} }) unless $epoch;
			push @terms, 'UNIX_TIMESTAMP(imported) ' . $term_hash->{op} . ' ?';
			push @values, $epoch;
		}
		if (@terms){
			push @clauses, '(' . join(' AND ', @terms) . ') ';
		}
		@terms = ();
		foreach my $term_hash (@{ $date_terms{or} }){
			my $epoch = UnixDate(ParseDate($term_hash->{value}), '%s');
			$epoch = $epoch + $self->parser->timezone_diff($epoch);	
			throw(400, 'Invalid import date', { term => $term_hash->{value} }) unless $epoch;
			push @terms, 'UNIX_TIMESTAMP(imported) ' . $term_hash->{op} . ' ?';
			push @values, $epoch;
		}
		if (@terms){
			push @clauses, '(' . join(' OR ', @terms) . ') ';
		}
		@terms = ();
		foreach my $term_hash (@{ $date_terms{not} }){
			my $epoch = UnixDate(ParseDate($term_hash->{value}), '%s');
			$epoch = $epoch + $self->parser->timezone_diff($epoch);	
			throw(400, 'Invalid import date', { term => $term_hash->{value} }) unless $epoch;
			push @terms, 'NOT UNIX_TIMESTAMP(imported) ' . $term_hash->{op} . ' ?';
			push @values, $epoch;
		}
		if (@terms){
			push @clauses, '(' . join(' AND ', @terms) . ') ';
		}
		$query .= join(' AND ', @clauses);
		
		$self->log->trace('import date search query: ' . $query);
		$self->log->trace('import date search values: ' . Dumper(\@values));
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my $counter = 0;
		while (my $row = $sth->fetchrow_hashref){
			push @id_ranges, { boolean => 'and', values => [ $row->{first_id}, $row->{last_id} ], import_info => $row };
			$counter++;
		}
		unless ($counter){
			$self->log->trace('No matching imports found for dates given');
			return [];
		}
	}
	
	# Handle name/description
	foreach my $term_hash ($self->parser->all_import_search_terms){
		next if $term_hash->{field} eq 'date';
		my @values;
		if ($term_hash->{field} eq 'id'){
			$query = 'SELECT * from ' . $db_name . '.imports WHERE NOT ISNULL(first_id) AND NOT ISNULL(last_id) AND id ' . $term_hash->{op} . ' ?';
			@values = ($term_hash->{value});
		}
		else {
			$query = 'SELECT * from ' . $db_name . '.imports WHERE NOT ISNULL(first_id) AND NOT ISNULL(last_id) AND ' . lc($term_hash->{field}) . ' RLIKE ?';
			@values = ($self->_term_to_sql_term($term_hash->{value}, $term_hash->{field}));
		}
		$self->log->trace('import search query: ' . $query);
		$self->log->trace('import search values: ' . Dumper(\@values));
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my $counter = 0;
		
		while (my $row = $sth->fetchrow_hashref){
			push @id_ranges, { 
				boolean => $term_hash->{boolean}, 
				values => [ $row->{first_id}, $row->{last_id} ], 
				import_info => $row,
			};
			$counter++;
		}
		if ($term_hash->{boolean} eq 'and' and not $counter){
			$self->log->trace('No matching imports found for ' . $term_hash->{field} . ':' . $term_hash->{value});
			return [];
		}
	}
	my $taken = time() - $start;
	$self->stats->{import_range_search} = $taken;
	$self->id_ranges([ @id_ranges ]);
}

1;
