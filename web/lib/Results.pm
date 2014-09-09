package Results;
use Moose;
with 'MooseX::Traits';
use IO::Handle;
use IO::File;
use JSON;

our $Bulk_dir = '/tmp';
our $Unbatched_results_limit = 10_000;

# Object for storing query results
has 'results' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { _add_result => 'push', records_returned => 'count', all_results => 'elements', idx => 'get' });
has 'total_records' => (traits => [qw(Counter)], is => 'rw', isa => 'Int', required => 1, default => 0,
	handles => { inc_total => 'inc' });
has 'total_docs' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'bulk_file' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', handles => { is_bulk => 'count' });
has 'bulk_dir' => (is => 'rw', isa => 'Str', required => 1, default => $Bulk_dir);
has 'json' => (is => 'rw', isa => 'JSON', required => 1, default => sub { return JSON->new->allow_nonref->allow_blessed->pretty(0) });
has 'is_approximate' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'percentage_complete' => (is => 'rw', isa => 'Num', required => 1, default => 0);

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	unless ($params{total_records}){ # don't calculate if these are already provided
		if ($params{results} and ref($params{results}) and ref($params{results}) eq 'ARRAY'){
			$params{total_records} = scalar @{ $params{results} };
		}
	}
	return \%params;
}

sub TO_JSON {
	my $self = shift;
	return { 
		results => $self->results, 
		totalRecords => $self->total_records > $self->total_docs ? $self->total_records : $self->total_docs, 
		recordsReturned => $self->records_returned,
		approximate => $self->is_approximate,
	};
}

sub delete_record {
	my $self = shift;
	my $given_record = shift;
	for (my $i = 0; $i < @{ $self->results }; $i++){
		if ($self->results->[$i]->{id} eq $given_record->{id}){
			splice(@{ $self->results }, $i, 1);
			return 1;
		}
	}
	return 0;
}

sub remove_transforms {
	my $self = shift;
	my $ret = [];
	foreach my $row ($self->all_results){
		delete $row->{transforms};
		push @$ret, $row;
	}
	return $ret;
}

sub add_result {
	my $self = shift;
	my $record = shift;
	my $no_inc = shift;
	
	if ($self->is_bulk){
		$self->_bulk_write([$record]);
	}
	else {
		$self->_add_result($record);
		if ($self->records_returned >= ($Unbatched_results_limit + 1)){
			$self->bulk_file($self->_create_bulk_file(time()));
			$self->_bulk_write($self->results);
			$self->results([]);
		}	
	}
	$self->inc_total unless $no_inc;
}

sub add_results {
	my $self = shift;
	my $results = shift;
	
	foreach (@$results){
		$self->add_result($_);
	}
}

sub merge {
	my $self = shift;
	my $results_obj = shift;
	my $q = shift;
	
	# Increment our counters
	$self->total_records( $self->total_records + $results_obj->total_records);
	$self->total_docs( $self->total_docs + $results_obj->total_docs);
	
	# Add the actual results
	foreach ($results_obj->all_results){
		$self->add_result($_, 1);
	}
	
	# Sort
	my $asc = sub { $a->{_orderby} <=> $b->{_orderby} };
	my $desc = sub { $b->{_orderby} <=> $a->{_orderby} };
	my $sort_fn = $q->orderby_dir eq 'DESC' ? $desc : $asc;
	my @final = sort $sort_fn  @{ $self->results };
	
	# Trim to the given query limit, if query object is provided
	if ($q and $q->limit){
		if (@final <= $q->limit){
			$self->results([ @final ]);   
		}
		else {
			$self->results([ @final[0..($q->limit - 1)] ]);
		}
	}
	else {
		$self->results([ @final ]);
	}
	
	# Mark if approximate
	if ($results_obj->is_approximate > $self->is_approximate){
		$self->is_approximate($results_obj->is_approximate);
	}
}

sub close {
	my $self = shift;
	if ($self->is_bulk){
		$self->bulk_file->{fh}->close;
	}
}

sub get_bulk_file {
	my $name = shift;
	return $Bulk_dir . '/' . $name;
}

sub get_results {
	my ($self,$offset,$limit) = @_;
	$offset ||= 0;
	$limit ||= $Unbatched_results_limit;
	
	if ($self->is_bulk){
		# offset already done via file pointer
		return $self->_bulk_read($limit);
	}
	elsif ($limit > $self->records_returned){
		return $self->results;
	}
	else {
		my @ret;
		my $counter = 0;
		foreach ($self->all_results){
			next unless $counter >= $offset;
			push @ret, $_;
			last if scalar @ret >= $limit;
			$counter++;
		}
		return \@ret;
	}
}

sub keys {
	my $self = shift;
	my $record = shift;
	
	my @keys;
	foreach my $key (keys %$record){
		next if ref($record->{$key});
		push @keys, $key;
	}
	if (exists $record->{_fields} and ref($record->{_fields}) and ref($record->{_fields}) eq 'ARRAY'){
		foreach my $field_hash (@{ $record->{_fields} }){
			if (defined $field_hash->{field} and defined $field_hash->{value}){
				push @keys, $field_hash->{field};
			}
		}
	}
	
	return @keys;	
}

sub value {
	my $self = shift;
	my $record = shift;
	my $key = shift;
	my $value = shift;
	
	if (exists $record->{$key}){
		if (defined $value){
			$record->{$key} = $value;
		}
		return $record->{$key};
	}
	else {
		foreach my $field_hash (@{ $record->{_fields} }){
			if ($field_hash->{field} eq $key){
				if (defined $value){
					$field_hash->{value} = $value;
				}
				return $field_hash->{value};
			}
		}
	}
}

sub delete_key {
	my $self = shift;
	my $record = shift;
	my $key = shift;
	
	if (exists $record->{$key}){
		delete $record->{$key};
		return 1;
	}
	else {
		for (my $i = 0; $i < @{ $record->{_fields} }; $i++){
			if ($record->{_fields}->[$i]->{field} eq $key){
				splice(@{ $record->{_fields} }, $i, 1);
				return 1;
			}
		}
	}
}

sub _create_bulk_file {
	my ($self,$name) = @_;
	
	my $bulk = {};
	
	# Create a file to store our results in
	$bulk->{name} = $name . '.json';
	$bulk->{fullname} = $Bulk_dir . '/' .$bulk->{name};
	$bulk->{fh} = new IO::File;
	$bulk->{fh}->open($bulk->{fullname}, O_RDWR|O_TRUNC|O_CREAT) or die($!);
	$bulk->{fh}->binmode(1);
	
	return $bulk;
}

sub _open_bulk_file {
	my ($self,$name) = @_;
	
	my $bulk = {};
	
	$self->bulk_file->{fh} = new IO::File;
	$self->bulk_file->{fh}->open($self->bulk_file->{fullname}, O_RDWR) or die($!);
	$self->bulk_file->{fh}->binmode(1);	
}

sub _bulk_write {
	my ($self,$records) = @_;

	$self->json->pretty(0);
	foreach my $record (@$records){
		$self->bulk_file->{fh}->print($self->json->encode($record). "\n");
	}

}

sub _bulk_read {
	my ($self,$limit) = @_;
		
	unless ($self->bulk_file->{fh} and $self->bulk_file->{fh}->opened){
		$self->_open_bulk_file($self->bulk_file->{name});
	}
	my $bulk = $self->bulk_file->{fh};

	my @results;
	while (my $line = <$bulk>){
		chomp($line);
		push @results, $self->json->decode($line);
		last if scalar @results >= $limit;
	}
	return \@results;
}

package Results::Groupby;
use Moose;
extends 'Results';
with 'MooseX::Traits';

# Object for storing query results
has 'results' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} },
	handles => { all_groupbys => 'keys', groupby => 'get' });
	
sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	unless ($params{total_records}){ # don't calculate if these are already provided
		if ($params{results} and ref($params{results}) and ref($params{results}) eq 'HASH'){
			foreach my $groupby (keys %{ $params{results} }){
				$params{total_records} += scalar @{ $params{results}->{$groupby} };
			}
		}
	}
	return \%params;
}	

sub records_returned {
	my $self = shift;
	my $count = 0;
	foreach my $groupby ($self->all_groupbys){
		$count += scalar @{ $self->results->{$groupby} };
	}
	return $count;
}

sub delete_record {
	my $self = shift;
	my $given_record = shift;
	foreach my $groupby ($self->all_groupbys){
		for (my $i = 0; $i < @{ $self->results->{$groupby} }; $i++){
			if ($self->results->{$groupby}->[$i]->{_groupby} eq $given_record->{_groupby}){
				splice(@{ $self->results->{$groupby} }, $i, 1);
				return 1;
			}
		}
	}
	return 0;
}

sub add_result {
	my $self = shift;
	my $groupby = shift;
	my $record = shift;
	
	my $added = 0;
	foreach my $existing_record (@{ $self->results->{$groupby} }){
		if ($existing_record->{_groupby} eq $record->{_groupby}){
			$existing_record->{_count} += $record->{_count};
			$added++;
			last;
		}
	}
	push @{ $self->results->{$groupby} }, $record unless $added;
	$self->inc_total;
}
	
sub add_results {
	my $self = shift;
	my $results = shift;
	
	foreach my $groupby (keys %$results){
		foreach (@{ $results->{$groupby} }){
			$self->add_result($groupby, $_);
		}
	}
}

sub merge {
	my $self = shift;
	my $results_obj = shift;
	my $q = shift;
	
	# Increment our counters
	$self->total_records( $self->total_records + $results_obj->total_records);
	$self->total_docs( $self->total_docs + $results_obj->total_docs);
	
	# Add the actual results
	$self->add_results($results_obj->results);
	
	# Sort and trim
	my $total_records = 0;
	my %results;
	foreach my $groupby ($self->all_groupbys){
		my %agg;
		my %intvals;
		foreach my $row (@{ $self->results->{$groupby} }){
			$agg{ $row->{_groupby} } += $row->{_count};
			$intvals{ $row->{_groupby} } = $row->{intval};
		}
		my @tmp;
		foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
			$total_records += $agg{$key};
			push @tmp, { intval => $intvals{$key}, '_groupby' => $key, '_count' => $agg{$key} };
			last if $q and scalar @tmp >= $q->limit;
		}
		$results{$groupby} = [ @tmp ];
	}
	$self->results({ %results });
	
	# Mark if approximate
	if ($results_obj->is_approximate > $self->is_approximate){
		$self->is_approximate($results_obj->is_approximate);
	}
}

sub all_results {
	my $self = shift;
	my $ret = [];
	# Return a flattened array
	foreach my $groupby (keys %{ $self->results }){
		foreach (@{ $self->results->{$groupby} }){
			push @$ret, $_;
		}
	}
	if (wantarray){
		return @$ret;
	}
	else {
		return $ret;
	}
}

1;