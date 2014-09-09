package CustomLog;
use Moose;
extends 'Log::Log4perl::Layout::PatternLayout::Multiline';
use Log::Log4perl;

sub render { 
	my($self, $message, $category, $priority, $caller_level) = @_;
	
	$caller_level = 0 unless defined $caller_level;
    my $result = $self->Log::Log4perl::Layout::PatternLayout::render($message, $category, $priority, $caller_level + 1);
	
	# Strip newlines and replace with a single space.
	$result =~ s/[\n\r]+/\ /g;
	
	# Strip any undefs with square brackets
	$result =~ s/\=\"\[undef\]\"/\=\"\"/g;
    
    return $result;
}

1;