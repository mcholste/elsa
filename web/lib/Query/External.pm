package Query::External;
use Moose;
use Data::Dumper;
use Module::Pluggable sub_name => 'datasource_plugins', require => 1, search_path => [ qw(Datasource) ];
use Try::Tiny;
use Ouch qw(:trytiny);

extends 'Query';

has 'start' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'end' => (is => 'rw', isa => 'Int', required => 1, default => sub { time });

has 'system_datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
} });
has 'web_datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
	_system_web_queries_count => {
		query_template => 'SELECT %s FROM (SELECT username, timestamp, milliseconds, archive, num_results FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE %s) derived %s ORDER BY %s LIMIT %d,%d',
		fields => [
			{ name => 'username' },
			{ name => 'timestamp', type => 'timestamp', alias => 'timestamp' },
		],
	},
	_system_web_queries_time => {
		query_template => 'SELECT %s FROM (SELECT username, timestamp, milliseconds, archive, num_results FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE %s) derived %s ORDER BY %s LIMIT %d,%d',
		fields => [
			{ name => 'username' },
			{ name => 'timestamp', type => 'timestamp', alias => 'timestamp' },
			{ name => 'milliseconds', type => 'int', alias => 'count' }
		],
	},
} });
has 'data_datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
	_system_event_rates => { 
		alias => '_node_stats', # changed from %d to %s because of architecture problems with %d and unsigned integers
		dsn => 'dbi:mysql:database=%s',
		query_template => 'SELECT %s FROM (SELECT host_id, INET_NTOA(host_id) AS host, timestamp, class, count FROM host_stats t1 JOIN classes t2 ON (t1.class_id=t2.id) HAVING %s) derived %s ORDER BY %s LIMIT %d,%d',
		fields => [
			{ name => 'host_id', type => 'ip_int' },
			{ name => 'host' },
			{ name => 'timestamp', type => 'timestamp', alias => 'timestamp' },
			{ name => 'class' },
			{ name => 'count', type => 'int', alias => 'count' }
		],
	}
} });

sub BUILD {
	my $self = shift;
	
	# setup dynamic (config based) plugins
	if ($self->conf->get('transforms/database')){
		foreach my $db_lookup_plugin (keys %{ $self->conf->get('transforms/database') }){
			my $conf = $self->conf->get('transforms/database/' . $db_lookup_plugin);
			my $alias = delete $conf->{alias};
			my $metaclass = Moose::Meta::Class->create( 'Transform::' . $alias, 
				superclasses => [ 'Transform::Database' ],
			);
			foreach my $attr (keys %$conf){
				$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
			}
			# Set name
			$metaclass->add_attribute('name' => (is => 'rw', default => $db_lookup_plugin ) );
		}
	}
	
	# Setup system datasources
	foreach my $datasource_type (keys %{ $self->web_datasources }){
		my $template_conf = $self->web_datasources->{$datasource_type};
		$self->system_datasources->{$datasource_type} = [];
		my $conf = { 
			alias => $datasource_type,
			dsn => $self->conf->get('meta_db/dsn'), 
			username => $self->conf->get('meta_db/username'),
			password => $self->conf->get('meta_db/password'),
			query_template => $template_conf->{query_template},
			fields => $template_conf->{fields},
		};
		
		my $metaclass = Moose::Meta::Class->create( 'Datasource::' . $conf->{alias}, 
			superclasses => [ 'Datasource::Database' ],
		);
		foreach my $attr (keys %$conf){
			$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
		}
		# Set name
		$metaclass->add_attribute('name' => (is => 'rw', default => $conf->{alias} ) );
		$self->system_datasources->{$datasource_type} = 1;
	}
	foreach my $datasource_type (keys %{ $self->data_datasources }){
		my $template_conf = $self->data_datasources->{$datasource_type};
		$self->system_datasources->{$datasource_type} = [];
		my $conf = { 
			alias => $template_conf->{alias},
			dsn => sprintf($template_conf->{dsn}, $self->conf->get('data_db/db') ? $self->conf->get('data_db/db') : 'syslog'),
			username => $self->conf->get('data_db/username'),
			password => $self->conf->get('data_db/password'),
			query_template => $template_conf->{query_template},
			fields => $template_conf->{fields},
		};
		
		my $metaclass = Moose::Meta::Class->create( 'Datasource::' . $conf->{alias}, 
			superclasses => [ 'Datasource::Database' ],
		);
		foreach my $attr (keys %$conf){
			$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
		}
		# Set name
		$metaclass->add_attribute('name' => (is => 'rw', default => $conf->{alias} ) );
		push @{ $self->system_datasources->{$datasource_type} }, $conf->{alias};
		
	}
	$self->log->debug('$self->system_datasources: ' . Dumper($self->system_datasources));
	
	# Setup custom datasources
	if ($self->conf->get('datasources')){
		foreach my $datasource_class (keys %{ $self->conf->get('datasources') }){
			# Upper case the first letter
			my @class_name_letters = split(//, $datasource_class);
			$class_name_letters[0] = uc($class_name_letters[0]);
			my $datasource_class_name = join('', @class_name_letters);
			
			foreach my $datasource_plugin (keys %{ $self->conf->get('datasources/' . $datasource_class) }){
				my $conf = $self->conf->get('datasources/' . $datasource_class . '/' . $datasource_plugin);
				throw(500, 'No conf found for ' . 'datasources/' . $datasource_class . '/' . $datasource_plugin, { config => $datasource_plugin }) unless $conf;
				my $alias = delete $conf->{alias};
				$alias ||= $datasource_plugin;
				my $metaclass = Moose::Meta::Class->create( 'Datasource::' . $alias, 
					superclasses => [ 'Datasource::' . $datasource_class_name ],
				);
				foreach my $attr (keys %$conf){
					$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
				}
				# Set name
				$metaclass->add_attribute('name' => (is => 'rw', default => $datasource_plugin ) );
			}
		}
	}
	
	$self->datasource_plugins();

	return $self;
}

sub execute {
	my $self = shift;
	my $cb = shift;
	
	my $cache = CHI->new(driver => 'RawMemory', datastore => {});
	DATASOURCES_LOOP: foreach my $datasource_arg (sort keys %{ $self->datasources }){
		$datasource_arg =~ /(\w+)\(?([^\)]+)?\)?/;
		my $datasource = lc($1);
		my @given_args = $2 ? split(/\,/, $2) : ();
		
		# Check to see if this is a system group
		if ($self->system_datasources->{$datasource} and ref($self->system_datasources->{$datasource})){
			delete $self->datasources->{$datasource};
			foreach my $alias (@{ $self->system_datasources->{$datasource} } ){
				$self->datasources->{$alias} = 1;
			}
			return $self->execute($cb);
		}
		
		# Check to see if this is a group (kind of a datasource macro)
		if ($self->conf->get('datasource_groups')){
			foreach my $datasource_group_name (keys %{ $self->conf->get('datasource_groups') }){
				if ($datasource_group_name eq $datasource){
					delete $self->datasources->{$datasource_group_name};
					foreach my $datasource_config_reference (@{ $self->conf->get('datasource_groups/' . $datasource_group_name . '/datasources') }){
						$self->datasources->{$datasource_config_reference} = 1;
					}
					# Now that we've resolved the group into its subcomponents, we recurse to run those 
					return $self->execute($cb);
				}
			}
		}
		my $found = 0;
		foreach my $plugin ($self->datasource_plugins()){
			if ($plugin =~ /\:\:$datasource/i){
				$self->log->debug('loading plugin ' . $plugin);
				$found++;
				my %compiled_args;
				try {
					%compiled_args = (
						conf => $self->conf,
						log => $self->log, 
						cache => $cache,
						args => [ @given_args ]
					);
					my $plugin_object = $plugin->new(%compiled_args);
					my $cv = AnyEvent->condvar;
					$cv->begin(sub { $self->results->percentage_complete(100); $cb->() });
					$plugin_object->query($self, sub { $cv->end });
				}
				catch {
					my $e = $self->catch_any(shift);
					delete $compiled_args{user};
					delete $compiled_args{cache};
					delete $compiled_args{conf};
					delete $compiled_args{log};
					$self->log->error('Error creating plugin ' . $plugin . ' with args ' . Dumper(\%compiled_args));
					$self->log->debug('e: ' . Dumper($e));
					if (blessed($e)){
						push @{ $self->warnings }, $e;
					}
					else {
						$self->add_warning(500, $e, { datasource => $datasource });
					}
				
					$cb->();
					return;
				};
			}
		}
		throw(404, 'datasource ' . $datasource . ' not found', { datasource => $datasource }) unless $found;
	}
	#$cb->();
}

1;