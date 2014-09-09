package Export;
use Moose;
use Data::Dumper;
with 'Fields'; #for epoch2iso

# Base class for Result plugins

has 'columns' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { ['timestamp', 'epoch_time'] });
has 'grid' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has '_raw_results' => (is => 'rw', required => 1);
has 'results' => (is => 'rw', required => 1, default => '');
has 'mime_type' => (is => 'rw', isa => 'Str', required => 1, default => 'text/plain');
has 'extension' => (is => 'rw', isa => 'Str', required => 1, default => '.txt');

sub BUILDARGS {
	my ($class, %params) = @_;

	$params{_raw_results} = delete $params{results};
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	# Was this a result set of data rows or a groupby?
	if (ref($self->_raw_results) eq 'HASH'){
		$self->columns([ 'groupby', 'values' ]);
		my @grid;
		foreach my $groupby (sort keys %{ $self->_raw_results }){
			push @grid, { groupby => $groupby, values => join (',', map { $_->{_groupby} } @{ $self->_raw_results->{$groupby} }) };
		}
		$self->grid(\@grid);
	}
	elsif ($self->_raw_results->[0]->{groupby}){
		$self->columns([ 'count', 'groupby' ]);
		$self->grid($self->_raw_results);
	}
	else {
		foreach my $field_hash (@{ $self->_raw_results->[0]->{_fields} }){
			push @{ $self->columns }, $field_hash->{field};
		}
		push @{ $self->columns }, 'msg';
		
		foreach my $row (@{ $self->_raw_results }){
			my $grid_hash = { timestamp => Utils::epoch2iso($row->{timestamp}), epoch_time => $row->{timestamp}, msg => $row->{msg} };
			foreach my $field_hash (@{ $row->{_fields} }){
				$grid_hash->{ $field_hash->{field} } = $field_hash->{value};
			}
			push @{ $self->grid }, $grid_hash;
		}
	}
}

1;