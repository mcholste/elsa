package Import;
use Moose;
use Data::Dumper;
use DateTime;
use DateTime::Format::Strptime;
use File::Temp;
use Config::JSON;
use String::CRC32;
use Time::HiRes qw(time);
use IO::File;
use POSIX qw(strftime);
use DBI;
use FindBin;
use Socket;

# Include the directory this script is in
use lib $FindBin::Bin;
use Module::Pluggable sub_name => 'import_plugins', require => 1, search_path => [ qw( Importer ) ];
# For some very dumb reason, a mod_perl/Apache bug is very occasionally causing Module::Pluggable to fail without these
#   specific require statements per-plugin.
#require Importer::Unformatted;
#require Importer::Syslog;
#require Importer::CSV;

my $absolute_path = $INC{'Import.pm'};
$absolute_path =~ s/Import\.pm$/\/Importer/;
opendir(DIR, $absolute_path);
while (my $file = readdir(DIR)){
	next unless $file =~ /\.pm$/;
	eval { require 'Importer/' . $file; };
	if ($@){
		warn('Unable to include Importer/' . $file . ': ' . $@);
	}
}
closedir(DIR);

has 'format' => (is => 'rw', isa => 'Str', required => 1, default => '0');
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => sub { 'Unnamed import' });
has 'description' => (is => 'rw', isa => 'Str', required => 1, default => sub { '' });
has 'infile' => (is => 'rw', isa => 'Str', required => 1);
has 'lines_to_skip' => (is => 'rw', isa => 'Int');
has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => ( is => 'ro', isa => 'Config::JSON', required => 1 );
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'timezone' => (is => 'rw', isa => 'Str', required => 1, default => 
	sub { DateTime::TimeZone->new( name => "local")->name });
has 'id' => (is => 'rw', isa => 'Int');
has 'lines_imported' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'program' => (is => 'rw', isa => 'Str', required => 1, default => sub { 'unknown' });
has 'start' => (is => 'rw', isa => 'Int');
has 'end' => (is => 'rw', isa => 'Int');
has 'class' => (is => 'rw', isa => 'Str');
has 'host' => (is => 'rw', isa => 'Str');
has 'year' => (is => 'rw', isa => 'Int');

#sub BUILDARGS {
#	my $class = shift;
#	my %params = @_;
#	
#	return \%params;
#}

sub BUILD {
	my $self = shift;
	
	my $db = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$db = $self->conf->get('syslog_db_name');
	}
		
	# Find our format/plugin
	my %best = (priority => 0);
	my @errors;
	foreach my $plugin_name ($self->import_plugins()){
		my %other_args;
		if ($self->class){
			$other_args{class} = $self->class;
		}
		if ($self->year){
			$other_args{year} = $self->year;
		}
		eval {
			my $plugin = $plugin_name->new(log => $self->log, conf => $self->conf, db => $self->db, %other_args);
			my $priority;
			if ($self->format()){
				if ($plugin->can($self->format())){
					my $method = $self->format();
					$priority = $plugin->$method();
				}
			}
			else {
				# Attempt to detect by filename
				$priority = $plugin->detect_filename($self->infile);
				unless ($priority){
					# Attempt to heuristically detect
					$priority = $plugin->heuristic($self->infile);
				}
			}
			next unless $priority;
			if ($priority > $best{priority}){
				$best{priority} = $priority;
				$best{plugin} = $plugin;
				$best{plugin_name} = $plugin_name;
			}
		};
		if ($@){
			# errors only matter if no valid plugin is found, because Unformatted should always work
			push @errors, $@;
			#$self->log->warn('Error building plugin: ' . $@);
		}
	}
	
	unless ($best{plugin}){
		$self->log->error('No plugin found to handle format ' . $self->format . ', errors: ' . join("\n", @errors));
		return $self;
	}
	$self->log->trace('Using importer plugin ' . $best{plugin_name} . ' to process file ' . $self->infile);
	
	my ($query,$sth);
	$query = 'INSERT INTO ' . $db . '.imports (name, description, datatype) VALUES(?,?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($self->name, $self->description, $self->format);
	$self->id($self->db->{mysql_insertid});
	
	my $start = time();
	my $lines_imported = $best{plugin}->process($self->infile, $self->program, $self->id, $self->host);
	my $end_time = time() - $start;
	$self->start($best{plugin}->start) if $best{plugin}->start;
	$self->end($best{plugin}->end) if $best{plugin}->end;
	$self->log->info("Import ID " . $self->id . " sent $lines_imported lines to ELSA in $end_time seconds");
	$self->lines_imported($lines_imported);
	
	return $self;
}

__PACKAGE__->meta->make_immutable;
