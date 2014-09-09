package Log;
use Moose::Role;
with 'MooseX::Log::Log4perl';
use Config::JSON;
use Sys::Hostname;

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );

around BUILDARGS => sub {
	my $orig = shift;
	my $class = shift;
	my %params = @_;
		
	if ($params{config_file}){
		$params{conf} = new Config::JSON ( $params{config_file} ) or die("Unable to open config file");
	}		
	
	config_logger($params{conf});
	$params{log} = Log::Log4perl::get_logger('App')
	  or die("Unable to init logger");
	
	return $class->$orig(%params);
};

no warnings;
*Log::Log4perl::Layout::PatternLayout::Multiline::render = sub {
	my($self, $message, $category, $priority, $caller_level) = @_;
	
	# Strip newlines and replace with a single space.
	$message =~ s/[\n\r]+/\ /g;
	
	# Strip any undefs with square brackets
	$message =~ s/\=\"\[undef\]\"/\=\"\"/g;

    $caller_level = 0 unless defined $caller_level;

    my $result = $self->Log::Log4perl::Layout::PatternLayout::render($message, $category, $priority, $caller_level + 1);
    return $result;
};
use warnings;

sub config_logger {
	my $config = shift;
	
	if (not ref $config){
		$config = new Config::JSON ( $config ) or die("Unable to open config file $config");
	}
	
	my $log_level = 'DEBUG';
	if ($ENV{DEBUG_LEVEL}){
		$log_level = $ENV{DEBUG_LEVEL};
	}
	elsif ($config->get('debug_level')){
		$log_level = $config->get('debug_level');
	}
	my $logdir = $config->get('logdir');
	my $tmpdir = $logdir . '/../tmp';
	my $logfile = 'node';
	if ($config->get('logfile')){
		$logfile = $config->get('logfile');
	}
	
	my $log_format = 'File, RFC5424';
	if ($config->get('log_format')){
		$log_format = $config->get('log_format');
	}
	
	my $hostname = hostname;
	
	my $log_conf = qq'
		log4perl.category.App       = $log_level, $log_format
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/$logfile.log 
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Syncer            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.Syncer.appender   = File
		log4perl.appender.Dat			 = Log::Log4perl::Appender::File
		log4perl.appender.Dat.filename  = $logdir/elsa.dat
		log4perl.appender.Dat.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Dat.layout.ConversionPattern = %d{e.SSSSSS}\0%p\0%M\0%F\0%L\0%P\0%m%n\1
		log4perl.appender.SyncerDat            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.SyncerDat.appender   = Dat
		log4perl.appender.RFC5424         = Log::Log4perl::Appender::Socket::UNIX
        log4perl.appender.RFC5424.Socket = $tmpdir/ops
        log4perl.appender.RFC5424.layout = Log::Log4perl::Layout::PatternLayout::Multiline
        log4perl.appender.RFC5424.layout.ConversionPattern = 1 %d{yyyy-MM-ddTHH:mm:ss.000}Z 127.0.0.1 elsa - 99 [elsa\@32473 priority="%p" method="%M" file="%F{2}" line_number="%L" pid="%P" hostname="$hostname"] %m%n
	';
	
	if (not Log::Log4perl->initialized()){
		Log::Log4perl::init( \$log_conf ) or die("Unable to init logger");
	}
}

1;