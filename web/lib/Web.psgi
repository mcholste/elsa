#!/usr/bin/perl
use strict;
use Data::Dumper;
use Plack::Builder;
use Plack::App::File;
use Plack::Builder::Conditionals;
use FindBin;
use lib $FindBin::Bin;

use Controller;
use Controller::Charts;
use Controller::Peers;
use View;
#use Web::Query;
use View::API;
use View::GoogleDatasource;
use View::GoogleDashboard;
use View::Mobile;

my $config_file = '/etc/elsa_web.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}

my $controller = Controller->new(config_file => $config_file) or die('Unable to start from given config file.');
my $charts_controller = Controller::Charts->new(config_file => $config_file) or die('Unable to start from given config file.');
my $peers_controller = Controller::Peers->new(config_file => $config_file) or die('Unable to start from given config file.');

my $auth;
if (lc($controller->conf->get('auth/method')) eq 'ldap' and $controller->conf->get('ldap')){
	require Authen::Simple::LDAP;
	$auth = Authen::Simple::LDAP->new(
		host        => $controller->conf->get('ldap/host'),
		binddn      => $controller->conf->get('ldap/bindDN'),
		bindpw      => $controller->conf->get('ldap/bindpw'),
		basedn        => $controller->conf->get('ldap/base'),
		filter => $controller->conf->get('ldap/filter') ? $controller->conf->get('ldap/filter') : '(&(objectClass=organizationalPerson)(objectClass=user)(sAMAccountName=%s))',
		log => $controller->log,
	);
}
elsif ($controller->conf->get('auth/method') eq 'local_ssh'){
	require Authen::Simple::SSH;
	$auth = Authen::Simple::SSH->new(
		host => 'localhost'
	);
}
elsif ($controller->conf->get('auth/method') eq 'local'){
	$controller->log->logdie('Auth method "local" is no longer implemented, halting.');
}
elsif ($controller->conf->get('auth/method') eq 'none'){
	# Inline a null authenticator
	package Authen::Simple::Null;
	use base qw(Authen::Simple::Adapter);
	__PACKAGE__->options({log => {
		type     => Params::Validate::OBJECT,
		can      => [ qw[debug info error warn] ],
		default  => Authen::Simple::Log->new,
		optional => 1}});
	sub check { my $self = shift; $self->log->debug('Authenticating: ', join(', ', @_)); return 1; }
	package main;
	$auth = Authen::Simple::Null->new(log => $controller->log);
}
elsif ($controller->conf->get('auth/method') eq 'db'){
	package Authen::Simple::ELSADB;
	use base qw(Authen::Simple::DBI);
	
	sub check {
		my ( $self, $username, $password ) = @_;
		my ( $dsn, $dbh, $sth ) = ( $self->dsn, undef, undef );

		unless ( $dbh = DBI->connect( $dsn, $self->username, $self->password, $self->attributes ) ) {
			my $error = DBI->errstr;
			$self->log->error( qq/Failed to connect to database using dsn '$dsn'. Reason: '$error'/ ) if $self->log;
		}
		
		$sth = $dbh->prepare($self->statement);
		$sth->execute($username, $password);
		my $row = $sth->fetchrow_arrayref;
		if ($row){
			return 1;
		}
		
		return 0;
    }
    
	package main;
	$auth = Authen::Simple::ELSADB->new(
		dsn => $controller->conf->get('auth_db/dsn') ? $controller->conf->get('auth_db/dsn') : $controller->conf->get('meta_db/dsn'),
		username =>	$controller->conf->get('auth_db/username') ? $controller->conf->get('auth_db/username') : $controller->conf->get('meta_db/username'),
		password => defined $controller->conf->get('auth_db/password') ? $controller->conf->get('auth_db/password') : $controller->conf->get('meta_db/password'),
		log => $controller->log,
		statement => $controller->conf->get('auth_db/auth_statement') ? $controller->conf->get('auth_db/auth_statement') : 'SELECT uid FROM users WHERE username=? AND password=PASSWORD(?)',
	);
}
elsif ($controller->conf->get('auth/method') eq 'security_onion'){
	# Inline a Security Onion authenticator
	package Authen::Simple::SecurityOnion;
	use base qw(Authen::Simple::DBI);
	sub check {
		my ( $self, $username, $password ) = @_;

		my ( $dsn, $dbh, $sth, $encrypted ) = ( $self->dsn, undef, undef, undef );

		unless ( $dbh = DBI->connect( $dsn, $self->username, $self->password, $self->attributes ) ) {
			my $error = DBI->errstr;
			$self->log->error( qq/Failed to connect to database using dsn '$dsn'. Reason: '$error'/ ) if $self->log;
		}
		
		my $salt = substr($password, 0, 2);
		my $query = 'SELECT username FROM user_info WHERE username=? AND password=CONCAT(SUBSTRING(password, 1, 2), SHA1(CONCAT(?, SUBSTRING(password, 1, 2))))';
		$sth = $dbh->prepare($query);
		$sth->execute($username, $password);
		my $row = $sth->fetchrow_arrayref;
		if ($row){
			return 1;
		}

		return 0;
    }

	package main;
	$auth = Authen::Simple::SecurityOnion->new(
		dsn => $controller->conf->get('auth_db/dsn') ? $controller->conf->get('auth_db/dsn') : $controller->conf->get('meta_db/dsn'),
		username =>	$controller->conf->get('auth_db/username') ? $controller->conf->get('auth_db/username') : $controller->conf->get('meta_db/username'),
		password => defined $controller->conf->get('auth_db/password') ? $controller->conf->get('auth_db/password') : $controller->conf->get('meta_db/password'),
		log => $controller->log,
		statement => ' ', #hardcoded in module above
	);
}
elsif (lc($controller->conf->get('auth/method')) eq 'kerberos' and $controller->conf->get('kerberos')){
	require Authen::Simple::Kerberos;
	$auth = Authen::Simple::Kerberos->new(
		realm => $controller->conf->get('kerberos/realm'),
		log => $controller->log,
	);
}
else {
	die('No auth method, please configure one!');
}

my $static_root = $FindBin::Bin . '/../';
if (exists $ENV{DOCUMENT_ROOT}){
	$static_root = $ENV{DOCUMENT_ROOT} . '/../';
}

builder {
	$ENV{PATH_INFO} = $ENV{REQUEST_URI}; #mod_rewrite will mangle PATH_INFO, so we'll set this manually here in case it's being used
	enable 'XForwardedFor';
	enable 'NoMultipleSlashes';
	enable 'Static', path => qr{^/?inc/}, root => $static_root;
	enable 'CrossOrigin', origins => '*', methods => '*', headers => '*';
	enable match_if all( path('!', qr!^/API/!) ), 'Session', store => 'File';
	unless ($controller->conf->get('auth/method') eq 'none'){
		enable match_if all( path('!', '/favicon.ico'), path('!', qr!^/inc/!), path('!', qr!^/transform!), path('!', qr!^/API/!) ), 'Auth::Basic', authenticator => $auth, realm => $controller->conf->get('auth/realm');
	}
	
	
	if ($controller->conf->get('api_only')){
		mount '/API' => View::API->new(controller => $peers_controller)->to_app;
	}
	else {
		mount '/API' => View::API->new(controller => $peers_controller)->to_app;
		mount '/favicon.ico' => sub { return [ 200, [ 'Content-Type' => 'text/plain' ], [ '' ] ]; };
		mount '/Query' => View->new(controller => $controller)->to_app;
		mount '/datasource' => View::GoogleDatasource->new(controller => $charts_controller)->to_app;
		mount '/dashboard' => View::GoogleDashboard->new(controller => $charts_controller)->to_app;
		mount '/Charts' => View->new(controller => $charts_controller)->to_app;
		mount '/m' => View::Mobile->new(controller => $controller)->to_app;
		mount '/' => View->new(controller => $controller)->to_app;
	}
};


