package Transform::Database;
use Moose;
use Moose::Meta::Class;
use Data::Dumper;
use CHI;
use DBI;
use JSON;
use URL::Encode qw(url_encode);
use Time::HiRes;
use Try::Tiny;
use Ouch qw(:trytiny);;
extends 'Transform';

our $Name = 'Database';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'dsn' => (is => 'rw', isa => 'Str', required => 1);
has 'username' => (is => 'rw', isa => 'Str', required => 1);
has 'password' => (is => 'rw', isa => 'Str', required => 1);
has 'query_template' => (is => 'rw', isa => 'Str', required => 1);
has 'query_placeholders' => (is => 'rw', isa => 'ArrayRef', required => 1);
has 'fields' => (is => 'rw', isa => 'ArrayRef', required => 1);

sub BUILD {
	my $self = shift;
	
	my $dbh = DBI->connect($self->dsn, $self->username, $self->password, { RaiseError => 1 });
	my ($query, $sth);
	if (scalar @{ $self->args }){
		$self->fields($self->args);
	}
	$query = sprintf($self->query_template, join(', ', @{ $self->fields }));
	$self->log->debug('query: ' . $query);
	$sth = $dbh->prepare($query);
		
	foreach my $datum (@{ $self->data }){
		$datum->{transforms}->{ $self->name } = {};
		
		my @placeholders;
		foreach my $col (@{ $self->query_placeholders }){
			if ($datum->{$col}){
				push @placeholders, $datum->{$col};
			}
		}
		#$self->log->debug('placeholders: ' . Dumper(\@placeholders));
		$sth->execute(@placeholders) or throw(500, $sth->errstr, { mysql => $query });
		my @rows;
		while (my $row = $sth->fetchrow_hashref){
			push @rows, $row;
		}
		
		foreach my $field (@{ $self->fields }){
			$datum->{transforms}->{ $self->name }->{$field} = [];
			foreach my $row (@rows){
				#$self->log->debug('row: ' . Dumper($row));
				foreach my $key (keys %$row){
					next unless $key eq $field and defined $row->{$key};
					#$self->log->debug('got new transform field: ' . $key . ' with value ' . $row->{$key});
					push @{ $datum->{transforms}->{ $self->name }->{$field} }, $row->{$key};
				}
			} 
		}
	}
		
	return $self;
}

 
1;
