package StatsWriter;
use Moose::Role;
use Module::Pluggable sub_name => 'stats_plugins', require => 1, search_path => [ qw(Stats) ], package => 'Controller';

has 'stat_objects' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

after BUILD => sub {
	my $self = shift;
	
	my $absolute_path = $INC{'StatsWriter.pm'};
	$absolute_path =~ s/StatsWriter\.pm$/\/StatsWriter/;
	warn 'absolute_path: ' . $absolute_path;
	return $self unless -d $absolute_path;
	opendir(DIR, $absolute_path);
	while (my $file = readdir(DIR)){
		next unless $file =~ /\.pm$/;
		eval { require 'StatsWriter/' . $file; };
		if ($@){
			warn('Unable to include StatsWriter/' . $file . ': ' . $@);
		}
	}
	closedir(DIR);
	
	$self->stats_plugins();
	
	foreach my $plugin_name ($self->stats_plugins()){
		my $plugin = $plugin_name->new(conf => $self->conf);
		push @{ $self->stat_objects }, $plugin;
	}
	
	return $self
};

#around BUILDARGS => sub {
#	my $orig = shift;
#	my $class = shift;
#	my %params = @_;
#		
#	if ($params{config_file}){
#		$params{conf} = new Config::JSON ( $params{config_file} ) or die("Unable to open config file");
#	}		
#	
#	
#	
#	return $class->$orig(%params);
#};

1;