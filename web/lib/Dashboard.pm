package Dashboard;
use Moose;

# Base class for Transform plugins
has 'api' => (is => 'rw', isa => 'Object', required => 1);
has 'user' => (is => 'rw', isa => 'User', required => 1);
has 'queries' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'data' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'start_time' => (is => 'rw', isa => 'Int', required => 1, default => (time() - 86400*7));
has 'end_time' => (is => 'rw', isa => 'Int', required => 1, default => (time()));
has 'groupby' => (is => 'rw', isa => 'Str', required => 1, default => 'hour');

sub _get_data {
	my $self = shift;
	
	foreach my $query (@{ $self->queries }){
		my $result = $self->api->query($query);
		push @{ $self->data }, [$query->{query_meta_params}->{comment}, $result->TO_JSON, $result->{groupby}->[0]];
	}
	
	return $self->data;
}

1;