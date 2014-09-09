package Dashboard::File;
use Moose;
use Data::Dumper;
use JSON;
use File::Slurp qw(slurp);
extends 'Dashboard';

has 'file' => (is => 'rw', isa => 'Str', required => 1);

sub BUILD {
	my $self = shift;
	
	if ($self->file =~ /\.csv$/){
		$self->_read_csv();
	}
	else {
		$self->_read_json();
	}
			
	return $self;
}

sub _read_csv {
	my $self = shift;
	
	open(FH, $self->file) or die($!);
	while (<FH>){
		next if $_ =~ /^#/;
		chomp;
		my @tmp_arr = split(/\,/, $_);
		my $description = shift @tmp_arr;
		my $query_string = join(',', @tmp_arr); # allows for commas in the query
		my $query_meta_params = {
			start => $self->start_time,
			end => $self->end_time,
			comment => $description,
		};
		
		$query_meta_params->{groupby} = [$self->groupby] unless $query_string =~ /\sgroupby[:=]/ or $query_string =~ /sum\([^\)]+\)$/;
		
		push @{ $self->queries }, {
			query_string => $query_string,
			query_meta_params => $query_meta_params,
			user => $self->user,
		};
	}
	close(FH);	
			
	return 1;
}

sub _read_json {
	my $self = shift;
	
	my $buf = slurp($self->file) or die('Invalid file: ' . $!);
	
	my $group_counter = 0;
	foreach my $dashboard_row (@{ $self->api->json->decode($buf) }){
		foreach my $chart (@{ $dashboard_row->{charts} }){
			foreach my $query (@{ $chart->{queries} }){
				my $query_meta_params = {
					start => $self->start_time,
					end => $self->end_time,
					comment => $query->{label},
					type => $chart->{type},
				};
				$query_meta_params->{groupby} = [$self->groupby] unless $query->{query} =~ /\sgroupby[:=]/ or $query->{query} =~ /sum\([^\)]+\)$/;
				$query->{query_string} = delete $query->{query};
				$query->{query_meta_params} = $query_meta_params;
				$query->{user} = $self->user;
			}
		}
		push @{ $self->queries }, $dashboard_row;
	}
			
	return 1;
}

1;