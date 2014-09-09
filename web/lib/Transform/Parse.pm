package Transform::Parse;
use Moose;
use Data::Dumper;
use Try::Tiny;
use Ouch qw(:trytiny);;
extends 'Transform';
our $Name = 'Parse';
has 'name' => (is => 'ro', isa => 'Str', required => 1, default => $Name);

sub BUILD {
	my $self = shift;
	
	my $pattern_name = $self->args->[0];
	my $patterns;
	
	# See if the pattern is in the config file
	if ($self->conf->get('transforms/parse/' . $pattern_name)){
		$patterns = $self->conf->get('transforms/parse/' . $pattern_name);
	}	 
	
	# See if the pattern is in user prefs and override with it
	if ($self->user->preferences and 
		$self->user->preferences->{tree} and
		$self->user->preferences->{tree}->{patterns} and 
		$self->user->preferences->{tree}->{patterns}->{$pattern_name}){
		$patterns = $self->user->preferences->{tree}->{patterns}->{$pattern_name};
	}
	throw(404, 'Unable to find pattern ' . $pattern_name, { pattern => $pattern_name }) unless $patterns;
	 
	$self->log->debug('patterns: ' . "\n" . Dumper($patterns));
	
	foreach my $pattern (@$patterns){	
		$pattern->{pattern} =~ s/\\\\/\\/g;
	}
	
	DATUM_LOOP: foreach my $record ($self->results->all_results){
		$record->{transforms}->{$Name} = {};
		
		foreach my $pattern (@$patterns){
			if (my @matches = $self->results->value($record, $pattern->{field}) =~ qr/$pattern->{pattern}/){
				for (my $i = 0; $i < @{ $pattern->{extractions} }; $i++){
					if (defined $matches[$i]){
						$record->{transforms}->{$Name}->{ $pattern->{field} }->{ $pattern->{extractions}->[$i] } = $matches[$i];
					}
				}
			}
		}
	}
	
	$self->log->debug('results: ' . Dumper($self->results));
	
	$self->on_transform->($self->results);
	
	return $self;
}
 
1;