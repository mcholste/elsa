package QueryParser;
use Moose;
use Moose::Util::TypeConstraints;
with 'MooseX::Traits';
with 'Utils';
with 'Fields';
with 'Warnings';
with 'MooseX::Clone';

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

our $QueryClasses = [qw(Query::Sphinx Query::External Query::SQL Query::Import)];
our $Max_limit = 10000;
our $Max_query_terms = 128;
our $Default_limit = 100;

use Results;
use User;
use Query::Sphinx;
use Query::External;
use Query::SQL;
use Query::Import;
use SyncMysql;

has 'user' => (is => 'rw', isa => 'User', required => 1);
has 'query_string' => (is => 'rw', isa => 'Str');
has 'meta_params' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'qid' => (is => 'rw', isa => 'Int');
has 'schedule_id' => (is => 'rw', isa => 'Int');
has 'peer_label' => (is => 'rw', isa => 'Str');
has 'from_peer' => (is => 'rw', isa => 'Str');
has 'implicit_plus' => (is => 'rw', isa => 'Bool', required => 1, default => 1);
has 'query_class' => (is => 'rw', isa => enum($QueryClasses));
has 'classes' => (is => 'rw', isa => 'HashRef' => required => 1, default => sub { return { map { $_ => {} } qw(given excluded distinct permitted partially_permitted groupby) } });
has 'query_term_count' => (is => 'rw', isa => 'Num', required => 1, default => 0);
has 'import_search_terms' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_import_search_terms' => 'count', 'all_import_search_terms' => 'elements' });
has 'program_translations' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'stopword_terms' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

# What we return
has 'directives' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { { 
	limit => $Default_limit,
	datasources => {},
	peers => {},
}});
has 'custom_directives' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'terms' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'transforms' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_transforms => 'count', all_transforms => 'elements', num_transforms => 'count', add_transforms => 'push' });
has 'connectors' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connectors => 'count', all_connectors => 'elements', num_connectors => 'count',
	connector_idx => 'get', add_connector => 'push' });
has 'highlights' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_warnings' => 'count', 'clear_warnings' => 'clear', 'all_warnings' => 'elements' });
has 'stats' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'on_connect' => (is => 'rw', isa => 'CodeRef');

# Object for deterministically parsing queries into query objects

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if ($params{qid}){
		my ($query, $sth);
		$query = 'SELECT username, query FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE qid=?';
		$sth = $params{db}->prepare($query);
		$sth->execute($params{qid});
		my $row = $sth->fetchrow_hashref;
		throw(404, 'Invalid qid ' . $params{qid}, { qid => $params{qid} }) unless $row;
		$params{q} = $row->{query};
		$params{user} = User->new(username => $row->{username}, conf => $params{conf});
	}
	if ($params{q}){
		# JSON-encoded query from web
		my $decode = $params{json}->decode($params{q});
		$params{query_string} = $decode->{query_string};
		$params{meta_params} = $decode->{query_meta_params};
		$params{raw_query} = delete $params{q};
	}
	elsif ($params{query_meta_params}){
		$params{meta_params} = delete $params{query_meta_params};
	}
	
	foreach my $property (qw(groupby timeout archive analytics datasources nobatch livetail)){
		if ($params{meta_params}->{$property}){
			$params{$property} = delete $params{meta_params}->{$property};
		}
	}
	
	unless ($params{user}){
		$params{user} = new User(username => 'system', conf => $params{conf});
		$params{log}->info('Defaulting user to system');
		if ($params{permissions}){
			$params{user}->permissions(ref($params{permissions}) ? $params{permissions} : $params{json}->decode($params{permissions}));
			$params{log}->trace('Set permissions: ' . Dumper($params{user}->permissions));
		}
	}
	
	return \%params;
}
 
sub BUILD {
	my $self = shift;
	
	$self->log->debug('meta_params: ' . Dumper($self->meta_params));
	
	$self->resolve_field_permissions($self->user);
	
	# Is this a system-initiated query?
	if ($self->schedule_id){
		$self->directives->{system} = 1;
	}
	elsif (not $self->peer_label and $self->user->username eq 'system'){
		$self->directives->{system} = 1;
	}
	
	if ($self->conf->get('query_timeout')){
		$self->directives->{timeout} = sprintf("%d", ($self->conf->get('query_timeout') * 1000));
		$self->directives->{max_query_time} = .9 * $self->directives->{timeout}; #90%
	}
	
	# Set known values here
	if ($self->meta_params->{archive}){
		$self->directives->{archive} = 1;
		$self->directives->{use_sql_regex} = 0;
	}
	if ($self->meta_params->{livetail}){
		$self->directives->{livetail} = 1;
	}
	
	# Override defaults for whether query terms are OR by default instead of AND by default
	if ($self->conf->get('default_or')){
		$self->implicit_plus(0);
	}
	
	# Override defaults with config for order of results
	if ($self->conf->get('default_sort_descending')){
		$self->directives->{orderby_dir} = 'DESC';
	}
	
	# Set a defaults if available in preferences
	if ($self->user->preferences and $self->user->preferences->{tree}->{default_settings} and
		$self->user->preferences->{tree}){
		my $prefs = $self->user->preferences->{tree}->{default_settings};
		if ($prefs->{orderby_dir}){
			$self->directives->{orderby_dir} = $prefs->{orderby_dir};
			$self->directives->{orderby} = 'timestamp';
		}
		if ($prefs->{timeout}){
			$self->directives->{timeout} = $prefs->{timeout};
		}
		if ($prefs->{default_or}){
			$self->implicit_plus(0);
		}
	}
	
	# Allow some directives to be sent in meta_params (so preferences can propagate from a parent peer)
	if ($self->meta_params->{orderby_dir}){
		$self->directives->{orderby_dir} = $self->meta_params->{orderby_dir};
	}
	if ($self->meta_params->{timeout}){
		$self->directives->{timeout} = $self->meta_params->{timeout};
	}
	if ($self->meta_params->{default_or}){
		$self->implicit_plus(0);
	}
	
	unless ($self->meta_info){
		$self->_get_info(sub {
			my $ret = shift;
			#$self->log->debug('got info: ' . Dumper($ret));
			$self->meta_info($ret);
			$self->on_connect->($self);
		});
	}
		
	return $self;	
}

sub parse {
	my $self = shift;
	my $given_query_class = shift;
	my $given_qid = shift;
	
	# Parse first to see if limit gets set which could incidate a batch job
	$self->_parse_query();
	
	$self->log->trace("Using timeout of " . $self->directives->{timeout});
	
	if ($given_query_class){
		$self->log->info('Overriding query class decision with given class ' . $given_query_class);
		$self->query_class($given_query_class);
	}
	else {
		$self->query_class($self->_choose_query_class);
	}
	
	$self->stats->{get_info} = $self->meta_info->{took};
	
	$self->log->trace('Creating new query of class ' . $self->query_class);
	
	my %args = (
		user => $self->user,
		conf => $self->conf,
		log => $self->log,
		db => $self->db,
		json => $self->json,
		parser => $self,
		query_string => $self->query_string,
		meta_params => $self->meta_params,
		highlights => $self->highlights,
		stats => $self->stats,
		directives => $self->directives,
		custom_directives => $self->custom_directives,
		terms => $self->terms,
		transforms => $self->transforms,
		connectors => $self->connectors,
		warnings => $self->warnings,
	);
	if ($given_qid){
		$args{qid} = $given_qid;
	}
	my $q = $self->query_class->new(%args);
	
	return $q;
}

sub _choose_query_class {
	my $self = shift;
	
	# Do we have indexed data for this time period?
	if ($self->directives->{start} and $self->directives->{start} < $self->meta_info->{indexes_min}){
		if ($self->{directives}->{start} > $self->meta_info->{archive_min}
			and not $self->conf->get('disallow_sql_search')){
			$self->log->trace('Choosing Query::SQL because start was less than indexes_min');
			return 'Query::SQL';
		}
#		else {
#			my $msg = 'Adjusting query time to earliest index date ' . scalar localtime($self->meta_info->{indexes_min});
#			$self->log->info($msg);
#			#$self->add_warning(200, $msg);
#		}
	}
	elsif ($self->directives->{end} and $self->directives->{end} < $self->meta_info->{indexes_min}){
		if ($self->{directives}->{end} < $self->meta_info->{archive_max}
			and not $self->conf->get('disallow_sql_search')){
			$self->log->trace('Choosing Query::SQL because end was less than indexes_max');
			return 'Query::SQL';
		}
#		else {
#			my $msg = 'Adjusting query time to latest index date ' . scalar localtime($self->meta_info->{indexes_max});
#			$self->log->info($msg);
#			#$self->add_warning(200, $msg);
#		}
	}
	
	# Batch if we're allowing a huge number of results
	if (not $self->directives->{nobatch} and not $self->directives->{groupby} and ($self->directives->{limit} == 0 or $self->directives->{limit} > $Results::Unbatched_results_limit)){
		$self->directives->{batch} = q{Batching because an unlimited number or large number of results has been requested.};
		$self->log->info($self->directives->{batch});
	}
	
	if ($self->has_import_search_terms and not $self->index_term_count){
		return 'Query::Import';
	}
	elsif (scalar keys %{ $self->directives->{datasources} } and not $self->directives->{datasources}->{sphinx}){
		return 'Query::External';
	}
	elsif ($self->directives->{livetail}){
		throw(400, 'Livetail not currently supported', { term => 'livetail' });
		#return 'Query::Livetail';
	}
	elsif ($self->directives->{archive}){
		$self->directives->{use_sql_regex} = 0;
		return 'Query::SQL';
	}
	elsif (not $self->index_term_count){
		# Skip Sphinx, execute a raw SQL search
		$self->log->info('No query terms, executing against raw SQL');
		$self->add_warning(200, 'No query terms, query did not use an index', { indexed => 0 });
		return 'Query::SQL';
	}
	else {
		return 'Query::Sphinx';
	}
			
	throw(500, 'Unable to determine query class', {});
}

sub _parse_query {
	my $self = shift;
	
	my $raw_query = $self->query_string;
		
	foreach my $class_id (sort keys %{ $self->user->permissions->{fields} }){
		$self->classes->{partially_permitted}->{$class_id} = 1;
	}
	$self->log->trace('partially_permitted_classes: ' . Dumper($self->classes->{partially_permitted}));
	
	# Strip off any connectors and apply later
	($raw_query, my @connectors) = split(/\s*\>\s+/, $raw_query);
	my @connector_params;
	foreach my $raw_connector (@connectors){
		#TODO cleanup this regex crime against humanity below
		$raw_connector =~ /([^\(]+)\(?( [^()]*+ | (?0) )\)?$/x;
		$self->add_connector($1);
		$self->log->trace("Added connector $1");
		my $raw_params = $2;
		if ($raw_params){
			$raw_params =~ s/\)$//;
			my @masks = $raw_params =~ /([\w]+\( (?: [^()]*+ | (?0) ) \))/gx;
			my $clone = $raw_params;
			foreach my $mask (@masks){
				$clone =~ s/\Q$mask\E/__MASK__/;
			}
			my @connector_params = split(/\s*,\s*/, $clone);
			foreach my $mask (@masks){
				$connector_params[0] =~ s/__MASK__/$mask/;
			}
			$self->add_connector_params([@connector_params]);
			$self->log->trace("Added connector params " . Dumper(\@connector_params));
		}
		
	}
		
	# Strip off any transforms and apply later
	($raw_query, my @transforms) = split(/\s*\|\s+/, $raw_query);
	
	# Make sure that any lone lowercase 'or' terms are uppercase for DWIM behavior
	$raw_query =~ s/\sor\s/ OR /gi;
	
	$self->log->trace('query: ' . $raw_query . ', transforms: ' . join(' ', @transforms));
	$self->add_transforms(@transforms);
	
	# See if there are any connectors given
	if ($self->meta_params->{connector}){
		my $connector = $self->meta_params->{connector};
		$self->add_connector($connector);
		$self->add_connector_params($self->meta_params->{connector_params});
	}
		
	# Check to see if the class was given in meta params
	if ($self->meta_params->{class}){
		my $class_name = $self->meta_info->{classes}->{ uc($self->meta_params->{class}) };
		my $class_value = sprintf("%d", $self->meta_info->{classes}->{ uc($self->meta_params->{class}) });
		$self->terms->{and}->{ $class_name . ':' . $class_value } = { field => $class_name, op => ':', value => $class_value };
	}
		
	# Check for meta limit
	if (exists $self->meta_params->{limit}){
		$self->directives->{limit} = (sprintf("%d", $self->meta_params->{limit}));
		$self->log->debug("Set limit " . $self->directives->{limit});
	}
	
	if (defined $self->meta_params->{start}){
		my $tz_diff = $self->timezone_diff($self->meta_params->{start});
		if ($self->meta_params->{start} =~ /^\d+(?:\.\d+)?$/){
			$self->directives->{start} = int($self->meta_params->{start});
		}
		else {
			$self->log->debug('Started with ' . $self->meta_params->{start} . ' which parses to ' . 
				UnixDate(ParseDate($self->meta_params->{start}), "%s"));
			my $start = UnixDate(ParseDate($self->meta_params->{start}), "%s") + $tz_diff;
			$self->log->debug('ended with ' . $start);
			$self->directives->{start} = $start;
			$self->meta_params->{start} = $start;
		}
	}
	if (defined $self->meta_params->{end}){
		my $tz_diff = $self->timezone_diff($self->meta_params->{end});
		if ($self->meta_params->{end} =~ /^\d+(?:\.\d+)?$/){
			$self->directives->{end} = int($self->meta_params->{end});
		}
		else {
			#my $end = UnixDate(ParseDate($self->meta_params->{end}), "%s");
			my $end = UnixDate(ParseDate($self->meta_params->{end}), "%s") + $tz_diff;
			$self->directives->{end} = $end;
			$self->meta_params->{end} = $end;
		}
	}
	
		
	if ($raw_query =~ /\S/){ # could be meta_attr-only
		$self->_parse_query_string($raw_query);
	}
	else {
		throw(400,'No query terms given', { query_string => '' });
	}
	
	my $num_added_terms = 0;
	my $num_removed_terms = 0;
	
#	$num_removed_terms += $self->_check_for_stopwords();
	
	# This will error out if anything is not permitted
	$self->_check_permissions();
	
	$self->log->trace("terms: " . Dumper($self->terms));
	
	$self->log->debug('count_terms: ' . $self->index_term_count());
	
	if ($self->_has_positive_terms or $self->has_import_search_terms){
		# ok
	}
	else {
		$self->log->debug('terms: ' . Dumper($self->terms));
		throw(400, 'No positive value in query.', { query_string => $self->terms });
	}
	
#	# Failsafe for times
#	if ($self->meta_params->{start} or $self->meta_params->{end}){
#		unless ($self->directives->{start}){
#			$self->directives->{start} = 0;
#			$self->log->trace('set start to 0');
#		}
#		unless ($self->directives->{end}){
#			$self->directives->{end} = time();
#			$self->log->trace('set end to ' . time());
#		}
#	}
#	
#	# Final sanity check
#	unless (defined $self->directives->{start} and $self->directives->{end} and $self->directives->{start} <= $self->directives->{end}){
#		throw(416, 'Invalid start or end: ' . (scalar localtime($self->directives->{start})) . ' ' . (scalar localtime($self->directives->{end})), { start => $self->directives->{start}, end => $self->directives->{end} });
#	}
	
	if (exists $self->directives->{start} or $self->directives->{end}){
		$self->log->debug('going with times start: ' . (scalar localtime($self->directives->{start})) .  ' (' . $self->directives->{start} . ') and end: ' .
			(scalar localtime($self->directives->{end})) . ' (' . $self->directives->{end} . ')');
	}
	
	# Exclude our from_peer
	if ($self->from_peer and $self->from_peer ne '_external'){
		$self->directives->{peers}->{excluded}->{ $self->from_peer } = 1;
	}
	
	# Default groupby to DESC order
	if ($self->directives->{groupby} and not $self->directives->{orderby_dir}){
		$self->directives->{orderby_dir} = 'DESC';
	}
	
	return 1;
}


sub _parse_query_string {
	my $self = shift;
	my $raw_query = shift;
	my $effective_operator = shift;
	
	my $qp = new Search::QueryParser(rxTerm => qr/[^\s()]+/, rxField => qr/[\w,\.]+/);
	# Special case for a lone zero
	if ($raw_query eq '0'){
		$raw_query = '"0"';
	}
	my $orig_parsed_query = $qp->parse($raw_query, $self->implicit_plus) or throw(400, $qp->err, { query_string => $raw_query });
	$self->log->debug("orig_parsed_query: " . Dumper($orig_parsed_query));
	
	my $parsed_query = dclone($orig_parsed_query); #dclone so recursion doesn't mess up original
	
	# Override any operators with the given effective operator
	if ($effective_operator){
		foreach my $op (keys %$parsed_query){
			my $arr = delete $parsed_query->{$op}; 
			$parsed_query->{$effective_operator} ||= [];
			push @{ $parsed_query->{$effective_operator} }, @$arr;
		}
		$self->log->debug("$parsed_query: " . Dumper($parsed_query));
	}
	
	# Recursively parse the query terms
	$self->_parse_query_term($parsed_query);
}


sub _parse_query_term {
	my $self = shift;
	my $terms = shift;
	my $given_operator = shift;
	
	$self->log->debug('terms: ' . Dumper($terms));
	
	foreach my $operator (keys %{$terms}){
		my $effective_operator = $operator;
		if ($given_operator){
			if ($given_operator eq '-' and ($effective_operator eq '' or $effective_operator eq '+')){
				$effective_operator = '-'; # invert the AND or OR
			}
			elsif ($given_operator eq '+' and $effective_operator eq '-'){
				$effective_operator = '-';
			}
		}
		
		my $arr = $terms->{$operator};
		foreach my $term_hash (@{$arr}){
			next unless defined $term_hash->{value};
			
			# Recursively handle parenthetical directives
			if (ref($term_hash->{value}) eq 'HASH'){
				$self->_parse_query_term($term_hash->{value}, $effective_operator);
				next;
			}
			
			if ($term_hash->{value} =~ /^\$(\w+)/){
				$self->log->debug('got macro ' . $1);
				$self->_parse_query_string($self->_resolve_macro($1), ''); # macro needs a clean slate for its operator
				next;
			}
			
			# Make field lowercase
			$term_hash->{field} = lc($term_hash->{field});
			
			if ($term_hash->{field} eq 'start'){
				# special case for start/end
				if ($term_hash->{value} =~ /^\d+$/){
					$self->directives->{start} = int($term_hash->{value});
				}
				else {
					my $tz_diff = $self->timezone_diff($term_hash->{value});
					$self->directives->{start} = UnixDate(ParseDate($term_hash->{value}), "%s") + $tz_diff;
				}
				$self->log->debug('start is now: ' . $self->directives->{start} .', ' . (scalar localtime($self->directives->{start})));
				next;
			}
			elsif ($term_hash->{field} eq 'end'){
				# special case for start/end
				if ($term_hash->{value} =~ /^\d+$/){
					$self->directives->{end} = int($term_hash->{value});
				}
				else {
					my $tz_diff = $self->timezone_diff($term_hash->{value});
					$self->directives->{end} = UnixDate(ParseDate($term_hash->{value}), "%s") + $tz_diff;
				}
				next;
			}
			elsif ($term_hash->{field} eq 'limit'){
				# special case for limit
				$self->directives->{limit} = sprintf("%d", $term_hash->{value});
				throw(400, 'Invalid limit', { term => 'limit' }) unless $self->directives->{limit} > -1;
				next;
			}
			elsif ($term_hash->{field} eq 'offset'){
				# special case for offset
				$self->directives->{offset} = sprintf("%d", $term_hash->{value});
				throw(400, 'Invalid offset', { term => 'offset' }) unless $self->directives->{offset} > -1;
				next;
			}
			elsif ($term_hash->{field} eq 'groupby'){
				if (defined $self->directives->{groupby}){
					throw(400, 'Only one groupby can be requested', { term => $term_hash->{value} });
				}
				my $value = lc($term_hash->{value});
				if (grep { $_ eq $value } @$Fields::Import_fields){
					throw(400, 'Cannot group by an import meta tag', { term => $value });
				}
				$self->directives->{groupby} = lc($value);
				$self->log->trace("Set groupby " . Dumper($self->directives->{groupby}));
				next;
			}
			elsif ($term_hash->{field} eq 'orderby'){
				my $value = lc($term_hash->{value});
				$self->directives->{orderby} = $value;
				$self->log->trace("Set orderby " . Dumper($self->directives->{orderby}));
				next;
			}
			elsif ($term_hash->{field} eq 'orderby_dir'){
				if (uc($term_hash->{value}) eq 'DESC'){
					$self->directives->{orderby_dir} = 'DESC';
				}
				elsif (uc($term_hash->{value}) eq 'ASC'){
					$self->directives->{orderby_dir} = 'ASC';
				}
				else {
					throw(400, 'Invalid orderby_dir', { term => $term_hash->{value} });
				}
				next;
			}
			elsif ($term_hash->{field} eq 'node'){
				if ($term_hash->{value} =~ /^[\w\.\:]+$/){
					if ($effective_operator eq '-'){
						$self->directives->{peers}->{excluded}->{ $term_hash->{value} } = 1;
					}
					else {
						$self->directives->{peers}->{given}->{ $term_hash->{value} } = 1;
					}
				}
				next;
			}
			elsif ($term_hash->{field} eq 'cutoff'){
				$self->directives->{limit} = $self->directives->{cutoff} = sprintf("%d", $term_hash->{value});
				throw(400, 'Invalid cutoff', { term => 'cutoff' }) unless $self->directives->{cutoff} > -1;
				$self->log->trace("Set cutoff " . $self->directives->{cutoff});
				next;
			}
			elsif ($term_hash->{field} eq 'datasource'){
				delete $self->directives->{datasources}->{sphinx}; # no longer using our normal datasource
				$self->directives->{datasources}->{ $term_hash->{value} } = 1;
				$self->log->trace("Set datasources " . Dumper($self->directives->{datasources}));
			}
			elsif ($term_hash->{field} eq 'nobatch'){
				$self->meta_params->{nobatch} = 1;
				$self->log->trace("Set batch override.");
				next;
			}
			elsif ($term_hash->{field} eq 'archive'){
				$self->directives->{archive} = $self->meta_params->{archive} = 1;
				$self->log->trace("Set archive.");
				next;
			}
			elsif ($term_hash->{field} eq 'analytics'){
				$self->directives->{analytics} = $self->meta_params->{analytics} = 1;
				$self->log->trace("Set analytics.");
				next;
			}
			
			my $orig_value = $term_hash->{value};
			if ($term_hash->{field} eq 'program' or $term_hash->{field} eq 'host' or $term_hash->{field} =~ /proto/){
				# Fine as is
			}
			elsif ($term_hash->{field} eq 'timeout'){
				# special case for timeout
				$self->directives->{timeout} = (int($term_hash->{value}) * 1000);
				throw(400, 'Invalid timeout', { term => 'timeout' }) unless $self->directives->{timeout} > -1;
				$self->directives->{max_query_time} = ($self->directives->{timeout} * .9);
				next;
			}
			
			
			$self->log->debug('term_hash value now: ' . $term_hash->{value});
			
			my $boolean = 'or';
				
			# Reverse if necessary
			if ($effective_operator eq '-' and $term_hash->{op} eq '!='){
				$boolean = 'and';
			}
			elsif ($effective_operator eq '-' and $term_hash->{op} eq '='){
				$boolean = 'not';
			}
			elsif ($effective_operator eq '+'){
				$boolean = 'and';
			}
			elsif ($effective_operator eq '-'){
				$boolean = 'not';
			}
									
			# Process a field/value or attr/value
			if ($term_hash->{field} and defined $term_hash->{value}){
				
				my $operators = {
					'>' => 1,
					'>=' => 1,
					'<' => 1,
					'<=' => 1,
					'!=' => 1, 
				};
				# Default unknown operators to AND
				unless ($operators->{ $term_hash->{op} }){
					$term_hash->{op} = '=';
				}
				
				if ($term_hash->{field} =~ /^import\_(\w+)/){
					throw(400, 'Invalid import field ' . $term_hash->{field}, { term => $term_hash->{field} }) unless grep { $_ eq $term_hash->{field} } @$Fields::Import_fields;
					push @{ $self->import_search_terms }, { field => $1, value => $term_hash->{value}, 
						op => $term_hash->{op}, boolean => $boolean };
					next;
				}
				
				# Range operators can have the same boolean and key and value as long as their operator is different
				my $op = ':';
				if ($term_hash->{op} =~ /[<>]/){
					$op .= $term_hash->{op};
				}
				
				$self->terms->{$boolean}->{ $term_hash->{field} . $op . ':' . $term_hash->{value} } = $term_hash;
				
				# Mark down any program translations
				if (lc($term_hash->{field}) eq 'program'){
					$self->program_translations->{ crc32( lc($term_hash->{value}) ) } = lc($term_hash->{value});
				}
			}
			else {
				$self->terms->{$boolean}->{ $term_hash->{field} . ':' . $term_hash->{value} } = $term_hash;
			}
		}
	}
	
	return 1;
}


sub _has_positive_terms {
	my $self = shift;
	my $given_boolean = shift;
	
	my @booleans;
	if ($given_boolean){
		@booleans = ($given_boolean);
	}
	else {
		@booleans = qw(and or);
	}
	
	my $count = 0;
	foreach my $boolean (@booleans){
		$count += scalar keys %{ $self->terms->{$boolean} };
	}
	
	return $count;
}

#sub _count_terms {
#	my $self = shift;
#	my $query_term_count = 0;
#		
#	foreach my $boolean (qw(or and)){
#		$query_term_count += scalar keys %{ $self->terms->{any_field_terms}->{$boolean} };
#		$query_term_count += scalar keys %{ $self->terms->{any_field_terms_sql}->{$boolean} };
#	}
#	foreach my $boolean (qw(or and)){
#		foreach my $class_id (keys %{ $self->terms->{field_terms}->{$boolean} }){
#			foreach my $field (keys %{ $self->terms->{field_terms}->{$boolean}->{$class_id} }){
#				$query_term_count += scalar @{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$field} };
#			}
#		}
#	}
#	
#	if ($self->terms->{field_terms_sql}){
#		foreach my $boolean (qw(or and)){
#			next unless $self->terms->{field_terms_sql}->{$boolean};
#			foreach my $class_id (keys %{ $self->terms->{field_terms_sql}->{$boolean} }){
#				foreach my $field (keys %{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id} }){
#					$query_term_count += scalar @{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id}->{$field} };
#				}
#			}
#		}
#	}
#	return $query_term_count;
#}

sub index_term_count {
	my $self = shift;
	my $query_term_count = $self->_has_positive_terms();
	
	return $query_term_count;
}

sub is_stopword {
	my $self = shift;
	my $keyword = shift;
	
	my $stopwords = $self->conf->get('stopwords');
	
	# Check all field terms to see if they are a stopword and warn if necessary
	if ($stopwords and ref($stopwords) and ref($stopwords) eq 'HASH'){
		if (exists $stopwords->{ lc($keyword) }){
			return 1;
		}
		elsif ($keyword =~ /^"([^"]+)"$/){
			my @possible_terms = split(/\s+/, $1);
			foreach my $term (@possible_terms){
				if (exists $stopwords->{ lc($term) }){
					$self->log->trace('Found stopword ' . $term . ' embedded in quoted term ' . $keyword);
					return 1;
				}
			}
		}
	}
	return 0;
}

sub _resolve_macro {
	my $self = shift;
	my $macro = shift;
	
	my ($query, $sth);
	
	$macro = lc($macro);
	
	# Create whois-based built-ins
	my %nets;
	my $subnets = $self->conf->get('transforms/whois/known_subnets');
	if ($subnets){
		foreach my $start (keys %$subnets){
			my $org = lc($subnets->{$start}->{org});
			$org =~ s/[^\w\_]//g;
			$nets{'src_' . $org } .= ' +srcip>=' . $start . ' +srcip<=' . $subnets->{$start}->{end};
			$nets{'dst_' . $org } .= ' +dstip>=' . $start . ' +dstip<=' . $subnets->{$start}->{end};
			$nets{$org} .= ' +srcip>=' . $start . ' +srcip<=' . $subnets->{$start}->{end} . ' +dstip>=' . $start . ' +dstip<=' . $subnets->{$start}->{end};
			$nets{src_local} .= ' +srcip>=' . $start . ' +srcip<=' . $subnets->{$start}->{end};
			$nets{dst_local} .= ' +dstip>=' . $start . ' +dstip<=' . $subnets->{$start}->{end};
		}
	}
		
	if ($self->user->username eq 'system'){
		# Try to find macro in available local prefs
		$query = 'SELECT * FROM preferences WHERE type=? AND name=? ORDER BY id DESC LIMIT 1';
		$sth = $self->db->prepare($query);
		$sth->execute('saved_query', $macro);
		my $row = $sth->fetchrow_hashref;
		return $row ? $row->{value} : '';
	}
	elsif ($self->user->preferences and $self->user->preferences->{tree}->{saved_query} and 
		$self->user->preferences->{tree}->{saved_query}->{$macro}){
		return $self->user->preferences->{tree}->{saved_query}->{$macro};
	}
	elsif (exists $nets{$macro}){
		return $nets{$macro};
	}
	else {
		$self->log->debug('macros available: ' . Dumper($self->user->preferences->{tree}));
		throw(400, 'Invalid macro (saved search): ' . $macro, { term => $macro });
	}
	
}

sub _term_to_sphinx_term {
	my $self = shift;
	my $class_id = shift;
	my $col = shift;
	my $value = shift;
	
	my $resolved_value = $self->normalize_value($class_id, $value, $Fields::Field_to_order->{$col});
	if ($value ne $resolved_value){
		return '(' . $value . '|' . $resolved_value . ')';
	}
	return $value;
}


sub _set_batch {
	my ( $self, $new_val, $old_val ) = @_;
	my ($query, $sth);
	$query = 'UPDATE query_log SET archive=? WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($new_val, $self->qid);
	return $sth->rows;
}

sub timezone_diff {
	my $self = shift;
	my $time = shift;
	
	# Apply client's timezone settings
	if (defined $self->meta_params->{timezone_offset}){
		# Find our offset in minutes to match Javascript's offset designation
		
		# Account for time given in epoch format
		if ($time =~ /^\d{10}$/){
			$time = 'epoch ' . $time;
		}
		my $server_offset_then = int(UnixDate(ParseDate($time), '%z')) / 100 * -60;
		my $server_offset_now = int(UnixDate(ParseDate('now'), '%z')) / 100 * -60;
		if ($self->meta_params->{timezone_offset} and $server_offset_then != $server_offset_now){
			my $dst_diff = $server_offset_then - $server_offset_now;
			$self->log->trace('Applying daylight savings time difference of ' . $dst_diff);
			$self->meta_params->{timezone_offset} += $dst_diff;
		}
		my $tz_diff = (($self->meta_params->{timezone_offset} - $server_offset_then) * 60);
		$self->log->trace('Applying timezone offset for ' . $time . ' of ' . $tz_diff);
		return $tz_diff;
	}
}

#sub _check_for_stopwords {
#	my $self = shift;
#	
#	my $num_removed_terms = 0;
#	my $stopwords = $self->conf->get('stopwords');
#	# Check all field terms to see if they are a stopword and warn if necessary
#	if ($stopwords and ref($stopwords) and ref($stopwords) eq 'HASH'){
#		$self->log->debug('checking terms against ' . (scalar keys %$stopwords) . ' stopwords');
#		foreach my $boolean (keys %{ $self->terms }){
#			foreach my $key (keys %{ $self->terms->{$boolean} }){
#				my $term = $self->terms->{$boolean}->{$key}->{value};
#				if ($self->is_stopword($term)){
#					if ($boolean eq 'or'){
#						throw(400, 'Query term ' . $self->terms->{or}->{$key}->{value} . ' is too common', { term => $self->terms->{or}->{$key}->{value} });
#					}
#					$self->log->trace('Found stopword: ' . $term);
#					$self->stopword_terms->{$term} = 1;
#					$num_removed_terms++;
#				}
#			}
#		}
#	}
#	
#	return $num_removed_terms;
#}

sub _check_permissions {
	my $self = shift;
	
	# Check for blanket allow on classes
	if ($self->user->permissions->{class_id}->{0} or $self->user->is_admin){
		$self->log->trace('User has access to all classes');
		return 0;
	}
	else {
		my $permitted_classes = { %{ $self->user->permissions->{class_id} } };
		
		# Drop any query terms that wanted to use a forbidden class
		foreach my $boolean (keys %{ $self->terms }){
			foreach my $key (keys %{ $self->terms->{$boolean} }){
				my ($field, $value) = ($self->terms->{$boolean}->{$key}->{field}, $self->terms->{$boolean}->{$key}->{value});
				my $forbidden;
				if ($field eq 'class' and not $self->user->permissions->{class_id}->{ $self->meta_info->{classes}->{$value} }){
					$forbidden = $value;
				}
				elsif ($field eq 'program' and not $self->user->permissions->{program_id}->{ crc32($value) }){
					$forbidden = $value;
				}
				elsif ($field eq 'host' and not $self->user->permissions->{host_id}->{ unpack('N*', inet_aton($value)) }){
					$forbidden = $value;
				}
				elsif ($self->user->permissions->{fields}->{$field} and not $self->user->permissions->{fields}->{$field} eq $value){
					$forbidden = $value;
				}
				
				if ($forbidden){
					$self->log->warn('Forbidding term ' . $key . ' with ' . Dumper($forbidden));
					throw(403, 'All terms for field ' . $field . ' were dropped due to insufficient permissions.', { term => $field });
				}
			}
		}
	}
}

__PACKAGE__->meta->make_immutable;