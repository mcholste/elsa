package SyncMysql;
use Moose;
use Data::Dumper;
use DBI;

has 'log' => ( is => 'ro', isa => 'Object', required => 1 );
has 'db_args' => (is => 'rw', isa => 'ArrayRef', required => 1);
has 'query_id' => (traits => ['Counter'], is => 'rw', isa => 'Num', required => 1, default => 1, handles => { next_id => 'inc' });
has 'watchers' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

our $Retries = 3;
our $Retry_period = 3;
our $Max_values = 4096;

sub BUILD {
	my $self = shift;
	# Validate connection
	DBI->connect(@{$self->db_args}) or die('cannot connect to ' . $self->db_args->[0] . ': ' . $DBI::errstr);
	
	return $self;
}

sub query {
	my $self = shift;
	my $query = shift;
	my $cb = pop;
	my @values = @_;
	
	$self->log->trace('query: ' . $query . "\nvalues: " . join(',', @values));
	
	my $attempts = 0;
	my $dbh;
	while ($attempts < $Retries){
		$attempts++;
		eval {
			$dbh = DBI->connect(@{$self->db_args}) or die($DBI::errstr);
			$attempts = $Retries;
		};
		if ($@){
			$self->log->error('Got connection error ' . $@);
			undef $dbh;
			sleep 1;
		}
	}
	unless ($dbh){
		$self->log->error('Unable to make a connection after ' . $Retries . ' attempts');
		$cb->(undef, $@, 0);
		return;
	}
		
	eval {
		# Make sure RaiseError is enabled so we can catch problems in this eval block
		$dbh->{RaiseError} = 1;
		my $sth = $dbh->prepare($query);
		$sth->execute(@values);
		my @rows;
		while (my $row = $sth->fetchrow_hashref){
			push @rows, $row;
		}
		$cb->($dbh, \@rows, 1);
		return;
	};
	if ($@){
		$self->log->error('Query: ' . $query . ' with values ' . join(',', @values) . ' got error ' . $@);
		$cb->(undef, $@, 0);
	}
}

sub multi_query {
	my $self = shift;
	my $query = shift;
	my $cb = pop;
	my @values = @_;
	
	
	my $attempts = 0;
	my $dbh;
	while ($attempts < $Retries){
		$attempts++;
		eval {
			$dbh = DBI->connect(@{$self->db_args}) or die($DBI::errstr);
			$attempts = $Retries;
		};
		if ($@){
			$self->log->error('Got connection error ' . $@);
			undef $dbh;
			sleep 1;
		}
	}
	unless ($dbh){
		$self->log->error('Unable to make a connection after ' . $Retries . ' attempts');
		$cb->(undef, $@, 0);
		return;
	}
		
	eval {
		# Make sure RaiseError is enabled so we can catch problems in this eval block
		$dbh->{RaiseError} = 1;
		my $sth = $dbh->prepare($query, { async => 1 });
		$sth->execute(@values);
		my @rows;
		do {
			while (my $row = $sth->fetchrow_hashref){
				push @rows, $row;
			}
		} while ($sth->more_results);
		$cb->(1, \@rows, 1);
		return;
	};
	if ($@){
		$self->log->error('Query: ' . $query . ' with values ' . join(',', @values) . ' got error ' . $@);
		$cb->(undef, $@, 0);
	}
}

# Sphinx needs a special query procedure to deal with the SHOW META query summary being tacked on to a multi-query
sub sphinx {
	my $self = shift;
	my $query = shift;
	my $cb = pop;
	my $attempts = shift or 0;
	my @values = @_;
	
	unless (scalar @values < $Max_values){
		my $errstr = 'Query contained ' . (scalar @values) . ' values which is more than max of ' . $Max_values;
		$self->log->error($errstr);
		$cb->(1, { rows => [], meta => { warning => $errstr } }, 1);
		return;
	}
		
	my $dbh;
	my @rows;
	my %meta;
		
	eval {
		$dbh = DBI->connect(@{$self->db_args}) or die($DBI::errstr);
		# Make sure RaiseError is enabled so we can catch problems in this eval block
		$dbh->{RaiseError} = 1;
				
		my $sth = $dbh->prepare($query, { async => 1 });
		$sth->execute(@values);
		do {
			while ($sth and my $row = $sth->fetchrow_hashref){
				# Is this a meta block row?
				if (exists $row->{Value} and exists $row->{Variable_name} and (scalar keys %$row) eq 2){
					next unless $row->{Variable_name};
					if ($row->{Variable_name} !~ /^keyword/ and $row->{Value} =~ /^\d+(?:\.\d+)?$/){
						$meta{ $row->{Variable_name} } += $row->{Value};
					}
					else {
						$meta{ $row->{Variable_name} } = $row->{Value};
					}
				}
				else {
					push @rows, $row;
				}
			}
		} while ($sth and $sth->more_results);
		#if (exists $meta{warning} and $meta{warning} !~ /(?:fullscan requires extern docinfo)|(?:estimated)/){
		if (exists $meta{warning} and not scalar @rows){
			die($meta{warning});
		}
		elsif($sth and $dbh) {
			# ok
		}
		else {
			# no warning and no sth/dbh, something went wrong, the error message will be visible in one of the other scopes
		}	
	};
	if ($@){
		$self->log->error('Got sphinx error ' . $@);
		undef $dbh;
		if ($@ =~ /max_query_time/ or $@ =~ /syntax/ or $@ =~ /query error/ or $@ =~ /query words mismatch/){
			# Fatal
			$self->log->warn('not retrying query due to type of error');
		}
		else {
			#sleep 1;
			$attempts++;
			$self->log->debug('attempts: ' . $attempts);
			if ($attempts < $Retries){
				my $w; $w = AnyEvent->timer(cb => sub { $self->sphinx($query, $attempts, @values, $cb); undef $w; }, after => $Retry_period);
				return;
			}
		}
	}
	
	unless ($dbh){
		my $errstr = $attempts > 1 ? 'Unable to make query after ' . $attempts . ' attempts, last error: ' . $@ : $@;
		$self->log->error($errstr);
		#$cb->(undef, $@, 0);
		#$cb->(1, { rows => [], meta => { warning => $errstr } }, 1);
		$cb->(undef, $errstr, 0);
		return;
	}
	
	$cb->(1, { rows => \@rows, meta => \%meta }, 1);
}

1;