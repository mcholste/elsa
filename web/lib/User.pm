package User;
use Moose;
use MooseX::Storage;
with 'Utils';
with 'MooseX::Traits';
with Storage('format' => 'Storable');
use DBI;
use Data::Dumper;
use Try::Tiny;
use Ouch qw(:trytiny);;

our @Serializable = qw(username uid permissions email groups extra_attrs is_admin session_start_time preferences); 

# Object for storing query results
has 'username' => (is => 'ro', isa => 'Str', required => 1);

has 'uid' => (is => 'rw', isa => 'Num');
has 'permissions' => (is => 'rw', isa => 'HashRef');
has 'preferences' => (is => 'rw', isa => 'HashRef');
has 'email' => (is => 'rw', isa => 'Str');
has 'ldap' => (is => 'rw', isa => 'Object');

has 'extra_attrs' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'groups' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { add_group => 'push' });
has 'is_admin' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'session_start_time' => (is => 'rw', isa => 'Num', required => 1, default => time());

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	unless ($params{username}){
		if (exists $ENV{HTTP_USER}){
			$params{username} = $ENV{HTTP_USER};
		}
		elsif ($params{conf}->get('auth/method') eq 'none'){
			$params{username} = 'user';
		}
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	$self->add_group($self->username);
	
	if ($self->username eq 'system'){
		$self->uid(1);
		$self->is_admin(1);
		$self->permissions({
			class_id => {
				0 => 1,
			},
			host_id => {
				0 => 1,
			},
			program_id => {
				0 => 1,
			},
			node_id => {
				0 => 1,
			},
			filter => '',
		});
		return $self;
	}
	elsif (lc($self->conf->get('auth/method')) eq 'none'){
		$self->_init_none();
	}
	elsif (lc($self->conf->get('auth/method')) eq 'ldap' ) {
		$self->_init_ldap();
	}
	elsif (lc($self->conf->get('auth/method')) eq 'local'){
		$self->_init_local();
	}
	elsif (lc($self->conf->get('auth/method')) eq 'db'){
		$self->_init_db();
	}
	elsif (lc($self->conf->get('auth/method')) eq 'security_onion'){
		$self->_init_security_onion();
	}
	elsif (lc($self->conf->get('auth/method')) eq 'kerberos' ) {
		$self->_init_kerberos();
	}
	else {
		throw(500, 'No auth_method', { config => 'auth method' });
	}
	
	# Apply allowed_groups check
	if ($self->conf->get('allowed_groups')){
		my $is_allowed = 0;
		ALLOWED_LOOP: foreach my $allowed_group (@{ $self->conf->get('allowed_groups') }){
			foreach my $group (@{ $self->groups }){
				$group =~ s/\A\s+//; 
				$group =~ s/\s+\z//;
				$allowed_group =~ s/\A\s+//; 
				$allowed_group =~ s/\s+\z//;
				if ($allowed_group eq $group){
					$is_allowed = $allowed_group;
					last ALLOWED_LOOP;
				}
			}
		}
		unless ($is_allowed){
			$self->log->error('User not found in any allowed group defined in config for "allowed_groups." User was in groups: ' . join(', ', @{ $self->groups }));
			return 0;
		}
	}
		
	# Get the uid
	my ( $query, $sth );
	$query = 'SELECT uid FROM users WHERE username=?';
	$sth   = $self->db->prepare($query);
	$sth->execute( $self->username );
	my $row = $sth->fetchrow_hashref;
	if ($row) {
		$self->uid($row->{uid});
	}
	else {
		# UID not found, so this is a new user and the corresponding user group,
		$self->log->debug('Creating user ' . $self->username);
		$self->_create_user() or throw(500, 'Unable to create new user ' . $self->username, { create_user =>  0 });
	}
	
	$self->permissions($self->_get_permissions())
		or ($self->log->error('Unable to get permissions') and return 0);
	$self->log->debug('got permissions: ' . Dumper($self->permissions));
	
	$self->preferences($self->_get_preferences());
	
	return $self;
}

sub _init_none {
	my $self = shift;
	
	if ($self->username ne 'user'){
		# Assume that we were given authentication via setting env var HTTP_USER (like via Apache built-in authentication) and use local database for settings
		throw(500, 'No db given', { config => 'db' }) unless $self->db;
		throw(500, 'No admin groups listed in admin_groups', { config => 'admin_groups' }) unless $self->conf->get('admin_groups');
		my ($query, $sth);
		$query = 'SELECT groupname FROM groups t1 JOIN users_groups_map t2 ON (t1.gid=t2.gid) JOIN users t3 ON (t2.uid=t3.uid) WHERE t3.username=?';
		$sth = $self->db->prepare($query);
		$sth->execute($self->username);
		while (my $row = $sth->fetchrow_hashref){
			push @{ $self->groups }, $row->{groupname};
		}
		# Is the group this user is a member of a designated admin group?
		foreach my $group (@{ $self->groups }){
			my @admin_groups = qw(root admin);
			if ($self->conf->get('admin_groups')){
				@admin_groups = @{ $self->conf->get('admin_groups') };
			}
			if ( grep { $group eq $_ } @admin_groups ){
				$self->log->debug( 'user ' . $self->username . ' is an admin');
				$self->is_admin(1);
				last;
			}
		}
				
		$query = 'SELECT email FROM users WHERE username=?';
		$sth = $self->db->prepare($query);
		$sth->execute($self->username);
		my $row = $sth->fetchrow_arrayref;
		if ($row){
			$self->email($row->[0]);
		}
	}
	else {
		$self->uid(2);
		$self->is_admin(1);
		$self->permissions({
			class_id => {
				0 => 1,
			},
			host_id => {
				0 => 1,
			},
			program_id => {
				0 => 1,
			},
			node_id => {
				0 => 1,
			},
			filter => '',
		});
		$self->email($self->conf->get('email/to') ? $self->conf->get('email/to') : 'root@localhost');
	}
}

sub _init_ldap {
	my $self = shift;
	
	require Net::LDAP::Express;
	require Net::LDAP::FilterBuilder;
	my $ldap = new Net::LDAP::Express(
		host        => $self->conf->get('ldap/host'),
		bindDN      => $self->conf->get('ldap/bindDN'),
		bindpw      => $self->conf->get('ldap/bindpw'),
		base        => $self->conf->get('ldap/base'),
		searchattrs => [ $self->conf->get('ldap/searchattrs') ],
	);
	unless ($ldap) {
		throw(502, 'Unable to connect to LDAP server', { ldap => $self->conf->get('ldap/host') });
	}
	$self->ldap($ldap);
	
	my $filter = sprintf( '(&(%s=%s))',
		$self->conf->get('ldap/searchattrs'), $self->username );
	my $result = $self->ldap->search( filter => $filter );
	my @entries = $result->entries();
	if ( scalar @entries > 1 ) {
		$self->log->error('Ambiguous response from LDAP server');
		return;
	}
	elsif ( scalar @entries < 1 ) {
		$self->log->error(
			'User ' . $self->username . ' not found in LDAP server' );
		return;
	}
	
	my $entry       = $entries[0];
	my $attr_map    = $self->conf->get('ldap/attr_map');
	my $extra_attrs = $self->conf->get('ldap/extra_attrs');
	ATTR_LOOP: foreach my $attr ( $entry->attributes() ) {
		$self->log->debug('checking attr: ' . $attr . ', val: ' . join(', ', $entry->get_value($attr)));
		if ($attr eq $attr_map->{email}){
			$self->email($entry->get_value($attr));
		}
		foreach my $normalized_attr ( keys %{$extra_attrs} ) {
			if ( $attr eq $extra_attrs->{$normalized_attr} ) {
				$self->extra_attrs->{$normalized_attr} =
				  $entry->get_value($attr);
				next ATTR_LOOP;
			}
		}
		if ( $attr eq $self->conf->get('ldap/groups_attr') ) {
			my @values = $entry->get_value($attr);
			foreach my $value (@values){
				$self->add_group($value);
			}
		}
		# Is the group this user is a member of a designated admin group?
		foreach my $group (@{ $self->groups }){
			if ( $self->conf->get('ldap/admin_groups')->{ $group } ){
				$self->log->debug( 'user ' . $self->username . ' is a member of admin group ' . $group );
				$self->is_admin(1);
				next ATTR_LOOP;
			}
		}
	}
	
	$self->_add_groups();
}

sub _init_kerberos {
	my $self = shift;
	if ($self->conf->get('kerberos/db_auth')){
		$self->_init_db();
	}
	else {
		$self->email($self->username . '@' . $self->conf->get('kerberos/realm'));
		$self->permissions({
			class_id => {
				0 => 1,
			},
			host_id => {
				0 => 1,
			},
			program_id => {
				0 => 1,
			},
			node_id => {
				0 => 1,
			},
			filter => '',
		});
		$self->is_admin(1);
	}
}

sub _init_local {
	my $self = shift;
	my %in;
	while (my @arr = getgrent()){
		my @members = split(/\s+/, $arr[3]);
		if (grep { $self->username } @members){
			$in{$arr[0]} = 1;
		}
	}
	setgrent(); # Resets the iterator to the beginning of the groups file for the next getgrent()
	#$self->log->debug('groups before: ' . Dumper($self->groups));
	$self->groups([ keys %in, $self->username ]);
	#$self->log->debug('groups after: ' . Dumper($self->groups));
	# Is the group this user is a member of a designated admin group?
	foreach my $group (@{ $self->groups }){
		my @admin_groups = qw(root admin);
		if ($self->conf->get('admin_groups')){
			@admin_groups = @{ $self->conf->get('admin_groups') };
		}
		if ( grep { $group eq $_ } @admin_groups ){
			$self->log->debug( 'user ' . $self->username . ' is an admin');
			$self->is_admin(1);
			last;
		}
	}
	$self->email($self->username . '@localhost');
}


sub _init_db {
	my $self = shift;
	
	throw(500, 'No db given', { config => 'db' }) unless $self->db;
	throw(500, 'No admin groups listed in admin_groups', { config => 'admin_groups' }) unless $self->conf->get('admin_groups');
	my ($query, $sth);
	$query = 'SELECT groupname FROM groups t1 JOIN users_groups_map t2 ON (t1.gid=t2.gid) JOIN users t3 ON (t2.uid=t3.uid) WHERE t3.username=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->username);
	while (my $row = $sth->fetchrow_hashref){
		push @{ $self->groups }, $row->{groupname};
	}
	# Is the group this user is a member of a designated admin group?
	foreach my $group (@{ $self->groups }){
		my @admin_groups = qw(root admin);
		if ($self->conf->get('admin_groups')){
			@admin_groups = @{ $self->conf->get('admin_groups') };
		}
		if ( grep { $group eq $_ } @admin_groups ){
			$self->log->debug( 'user ' . $self->username . ' is an admin');
			$self->is_admin(1);
			last;
		}
	}
	
	if ($self->conf->get('auth_db/email_statement')){
		$query = $self->conf->get('auth_db/email_statement');
		my $dbh = DBI->connect($self->conf->get('auth_db/dsn'), $self->conf->get('auth_db/username'), $self->conf->get('auth_db/password'), { RaiseError => 1 });
		my $sth = $dbh->prepare($query);
		$sth->execute($self->username);
		my $row = $sth->fetchrow_arrayref;
		if ($row){
			$self->email($row->[0]);
		}
	}
	else {
		$self->email($self->username . '@localhost');
	}
}

sub _init_security_onion {
    my $self = shift;
    $self->is_admin(1);

    my ($query, $sth);

    if ($self->conf->get('auth_db/email_statement')){
        $query = $self->conf->get('auth_db/email_statement');
        my $dbh = DBI->connect($self->conf->get('auth_db/dsn'), $self->conf->get('auth_db/username'), $self->conf->get('auth_db/password'), { RaiseError => 1 });
        my $sth = $dbh->prepare($query);
        $sth->execute($self->username);
        my $row = $sth->fetchrow_arrayref;
        if ($row){
            $self->email($row->[0]);
        }
    }
    else {
        $self->email($self->conf->get('email/to') ? $self->conf->get('email/to') : 'root@localhost');
    }
}

sub _create_user {
	my $self = shift;
	
	$self->log->info("Creating user " . $self->username);
	my ( $query, $sth );
	eval {
		$self->db->begin_work;
		$query = 'SELECT uid FROM users WHERE username=?';
		$sth   = $self->db->prepare($query);
		$sth->execute( $self->username );
		my $row = $sth->fetchrow_arrayref;
		if ($row){
			$self->log->warn('already created user ' . $self->username . ' with uid ' . $row->[0]);
		}
		else {	
			$query = 'INSERT INTO users (username) VALUES (?)';
			$sth   = $self->db->prepare($query);
			$sth->execute( $self->username );
			$query = 'INSERT IGNORE INTO groups (groupname) VALUES (?)';
			$sth   = $self->db->prepare($query);
			$sth->execute( $self->username );
			$query =
			    'INSERT INTO users_groups_map (uid, gid) SELECT ' . "\n"
			  . '(SELECT uid FROM users WHERE username=?),' . "\n"
			  . '(SELECT gid FROM groups WHERE groupname=?)';
			$sth = $self->db->prepare($query);
			$sth->execute( $self->username, $self->username );
		}

		my $select  = 'SELECT groupname FROM groups WHERE groupname=?';
		my $sel_sth = $self->db->prepare($select);
		$query = 'INSERT IGNORE INTO groups (groupname) VALUES (?)';
		$sth   = $self->db->prepare($query);
		foreach my $group ( @{ $self->groups } ) {
			$sel_sth->execute($group);
			my $row = $sel_sth->fetchrow_hashref;

			# Only do the insert if a previous entry did not exist
			unless ($row) {
				$sth->execute($group);
			}
		}

		$query = 'SELECT uid FROM users WHERE username=?';
		$sth   = $self->db->prepare($query);
		$sth->execute( $self->username );
		$row = $sth->fetchrow_hashref;
		if ($row) {
			$self->uid($row->{uid});
		}
		else {
			$self->log->error('Unable to find uid for user ' . $self->username );
			$self->db->rollback;
			return;
		}
		
		$self->db->commit;
	};
	if ($@) {
		$self->log->error( 'Database error: ' . $@ );
		eval { $self->db->rollback; };
		return;
	}
	return 1;
}

sub _add_groups {
	my $self = shift;
	my ( $query, $sth );
	return 0 unless scalar @{ $self->groups };
	
	$query = 'SELECT groupname FROM groups WHERE ' . join(' OR ', map { 'groupname=?' } @{ $self->groups });
	$sth = $self->db->prepare($query);
	$sth->execute(@{ $self->groups });
	my %existing;
	while (my $row = $sth->fetchrow_hashref){
		$existing{ $row->{groupname} } = 1;
	}
	
	$query = 'INSERT IGNORE INTO groups (groupname) VALUES (?)';
	$sth   = $self->db->prepare($query);
	foreach my $groupname (@{ $self->groups }){
		next if exists $existing{$groupname};
		$sth->execute($groupname);
		$self->log->info('Added group ' . $groupname);
	}
	
	return 1;
}	

sub _get_permissions {
	my ($self) = @_;
	
	my ($query, $sth);
		
	# Find group permissions
	my %permissions;
	ATTR_LOOP: foreach my $attr (qw(class_id host_id program_id node_id)){
		if ($self->is_admin){
			$permissions{$attr} = { 0 => 1 };
			next ATTR_LOOP;
		}
		else {
			$query =
			  'SELECT DISTINCT attr_id' . "\n" .
			  'FROM groups t1' . "\n" .
			  'LEFT JOIN permissions t2 ON (t1.gid=t2.gid)' . "\n" .
			  'WHERE attr=? AND t1.groupname IN (';
			my @values = ( $attr );
			my @placeholders;
			foreach my $group ( @{ $self->groups } ) {
				push @placeholders ,       '?';
				push @values, $group;
			}
			$query .= join( ', ', @placeholders ) . ')';
			$sth = $self->db->prepare($query);
			$sth->execute(@values);
			my @arr;
			while (my $row = $sth->fetchrow_hashref){
				# If at any point we get a zero, that means that all are allowed, no exceptions, so bug out to the next attr loop iter
				if ($row->{attr_id} eq '0' or $row->{attr_id} eq 0){
					$permissions{$attr} = { 0 => 1 };
					next ATTR_LOOP;
				}
				push @arr, $row->{attr_id};
			}
			# Special case for program/node which defaults to allow
			foreach my $allow_attr (qw(program_id node_id)){
				if (scalar @arr == 0 and $attr eq $allow_attr){
					$permissions{$attr} = { 0 => 1 };
					next ATTR_LOOP;
				}
			}
			$permissions{$attr} = { map { $_ => 1 } @arr };
		}
	}
	
	# Find field permissions
	$permissions{fields} = {};
	unless ($self->is_admin){
		$query =
			'SELECT DISTINCT attr, attr_id' . "\n" .
			'FROM groups t1' . "\n" .
			'LEFT JOIN permissions t2 ON (t1.gid=t2.gid)' . "\n" .
			'WHERE attr NOT IN ("class_id", "host_id", "program_id", "node_id") AND t1.groupname IN (';
		my @values;
		my @placeholders;
		foreach my $group ( @{ $self->groups } ) {
			push @placeholders ,       '?';
			push @values, $group;
		}
		$query .= join( ', ', @placeholders ) . ')';
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my @arr;
		while (my $row = $sth->fetchrow_hashref){
			my $field = $row->{attr};
			my $value = $row->{attr_id};
			$permissions{fields}->{$field} ||= [];
			push @{ $permissions{fields}->{$field} }, $value;
		}
	}
		
	#$self->log->debug('permissions: ' . Dumper(\%permissions));
	
	return \%permissions;
}

sub _get_preferences {
	my $self = shift;
	
	my ($query, $sth);
	$query = 'SELECT * FROM preferences WHERE uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->uid);
	
	my $prefs = {};
	my @grid;
	while (my $row = $sth->fetchrow_hashref){
		$row->{name} = unicode_escape($row->{name});
		$row->{type} = unicode_escape($row->{type});
		$row->{value} = unicode_escape($row->{value});
		push @grid, $row;
		$prefs->{ $row->{type} } ||= {};
		if ($row->{value} =~ /^[\[\{]/){
			eval {
				$row->{value} = $self->json->decode($row->{value});
			};
			if ($@){
				$self->log->error('Error decoding preference value ' . $row->{value} . ': ' . $@);
			}
		}	
		$prefs->{ $row->{type} }->{ $row->{name} } = $row->{value};
	}
	return { tree => $prefs, grid => \@grid };
}

sub TO_JSON {
	my $self = shift;
	my $export = {};
	foreach my $key (@Serializable){
		$export->{$key} = $self->$key;
	}
	$export->{config_file} = $self->conf->pathToFile;
	return $export;
}

sub pack { return shift->TO_JSON }
sub unpack { return @_ };

sub is_permitted {
	my ($self, $attr, $attr_id, $class_id) = @_;
	
	if ($self->permissions->{$attr}->{0} # all are allowed
		or $self->permissions->{$attr}->{$attr_id}){
		return 1;
	}
	# Handle field permissions
	elsif ($class_id){
		if ($self->permissions->{class_id}->{0}){
			return 1;
		}
		elsif (exists $self->permissions->{fields}->{$class_id}){	
			foreach my $hash (@{ $self->permissions->{fields}->{$class_id} }){
				foreach my $type (qw(attr field)){
					if ($attr eq $hash->{$type}->[0] and $attr_id eq $hash->{$type}->[1]){
						return 1;
					}
					# Handle range field permissions
					elsif ($hash->{$type}->[1] =~ /^(\d+)\-(\d+)$/){
						my ($min, $max) = ($1, $2);
						if (defined $min and defined $max and $min <= $attr_id and $attr_id <= $max){
							return 1;
						}
					}
				}
			}
		}
		$self->log->warn('forbidden: attr: ' . $attr . ', attr_id: ' . $attr_id . ', class_id: ' . $class_id);
		return 0;
	}
	else {
		foreach my $id (keys %{ $self->permissions->{$attr} }){
			if ($id =~ /^(\d+)\-(\d+)$/){
				my ($min, $max) = ($1, $2);
				if ($min <= $attr_id and $attr_id <= $max){
					return 1;
				}
			}
		}
		$self->log->debug('failed when checking attr ' . $attr . ' attr_id ' . $attr_id . ' with ' . Dumper($self->permissions->{$attr}));
		return 0;
	}
}

1;
