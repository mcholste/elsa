package Query::Sphinx;
use Moose;
use Data::Dumper;
use SyncMysql;
use Time::HiRes qw(time);
use AnyEvent;
use Try::Tiny;
use Ouch qw(:trytiny);
use Socket;
use String::CRC32;
use Sys::Hostname::FQDN;
use Net::DNS;
extends 'Query';
with 'Fields';

has 'data_db' => (is => 'rw', isa => 'HashRef');
has 'post_filters' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { { and => {}, not => {} } },
	handles => { has_postfilters => 'keys' });
has 'schemas' => (is => 'rw', isa => 'HashRef');

sub BUILD {
	my $self = shift;
	
	$self->schemas($self->_get_index_groups());
	
	return $self;
}

sub estimate_query_time {
	my $self = shift;
	
	my $query_time = 0;
	
	# AND terms x OR terms x individual classes x rows in indexes
	my $num_ands = scalar keys %{ $self->terms->{and} };
	my $num_ors = scalar keys %{ $self->terms->{or} };
	my $num_nots = scalar keys %{ $self->terms->{not} };
	my $total_terms = $num_ands + $num_ors + $num_nots;
	
	my $total_queries = 0;
	my $total_search = 0;
	my $rows_to_search = 0;
	my $num_indexes = 0;
	#my $total_hits = 0;
	
	foreach my $group_key (keys %{ $self->schemas }){
		my $indexes = $self->_get_index_list($self->schemas->{$group_key});
		my $queries = $self->_build_queries($group_key);
		$num_indexes += scalar @$indexes;
		$total_queries += scalar @$queries;

		foreach my $index_name (@$indexes){
			$rows_to_search += $self->schemas->{$group_key}->{$index_name}->{records};
			#$total_hits += $self->_get_keyword_hits($index_name, $queries);
		}
		
		next unless scalar @$indexes;
		$total_search += ($rows_to_search / (scalar @$indexes) * $total_queries * $total_terms);
	}
	
	my $ret = {
		indexes => $num_indexes,
		records => $rows_to_search,
		queries => $total_queries,
		and_terms => $num_ands,
		not_terms => $num_nots,
		or_terms => $num_ors,
		total_records_searched => $total_search,
		estimated_records_searched_per_second => 1_000_000_000,
		estimated_time => $total_search / 1_000_000_000,
	};
	
	$self->estimated($ret);
	
	return $ret;
}

#sub _get_keyword_hits {
#	my $self = shift;
#	my $index = shift;
#	my $queries = shift;
#	
#	my $total_hits = 0;
#	my %terms;
#	foreach my $query (@$queries){
#		my $where = $query->{where_clause};
#		my @match_terms = $where =~ /MATCH\(([^\)]+)/;
#		foreach (@match_terms){
#			next if /^\@/;
#			$terms{$_} = 1;
#		}
#	}
#	
#	my ($query, $sth);
#	
#	$query = "CALL KEYWORDS('', '$index', 1)";
#	$sth = $self->data_db->{sphinx} 
#	
#	return $total_hits;	
#}

sub _normalize_quoted_value {
	my $self = shift;
	my $hash = shift;
	
	if (defined $Fields::Field_to_order->{ $hash->{field} }){
		$hash->{value} =~ s/^["']//;
		$hash->{value} =~ s/["']$//;
		return $hash->{value};
	}
	
#	# Quoted integers don't work for some reason
#	if ($hash->{value} =~ /^['"][a-zA-Z0-9]+['"]$/){
#		$hash->{value} =~ s/^["']//;
#		$hash->{value} =~ s/["']$//;
#		return $hash->{value};
#	}
#	else {
#		return $hash->{value};
#	}

	return '"' . $hash->{value} . '"';
}

sub _normalize_terms {
	my $self = shift;
	
	# Normalize query terms
	foreach my $boolean (keys %{ $self->terms }){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			my $term_hash = $self->terms->{$boolean}->{$key};
			if($term_hash->{quote}){
				$term_hash->{value} = $self->_normalize_quoted_value($term_hash);
			}
			else {
				# Get rid of any non-indexed chars
				$term_hash->{value} =~ s/[^a-zA-Z0-9\.\@\-\_\\]/\ /g;
				# Escape backslashes followed by a letter
				$term_hash->{value} =~ s/\\([a-zA-Z])/\ $1/g;
				#$term_hash->{value} =~ s/\\\\/\ /g; # sphinx doesn't do this for some reason
				# Escape any '@' or sphinx will error out thinking it's a field prefix
				if ($term_hash->{value} =~ /\@/){
					# need to quote
					$term_hash->{value} = '"' . $term_hash->{value} . '"';
				}
				# Escape any hyphens
				#$term_hash->{value} =~ s/\-/\\\\\-/g;
				
			}
			
			if ($term_hash->{value} =~ /^"?\s+"?$/){
				my $err = 'Term ' . $term_hash->{value} . ' was comprised of only non-indexed chars and removed';
				$self->add_warning(400, $err, { term => $term_hash->{value} });
				$self->log->warn($err);
				next;
			}
			
			# Set case
			$term_hash->{field} = lc($term_hash->{field});
			if ($term_hash->{field} eq 'class'){
				$term_hash->{value} = uc($term_hash->{value});
			}
			else {
				#$term_hash->{value} = lc($term_hash->{value});
				$term_hash->{value} = $term_hash->{value};
			}
		}
	}
	
	return 1;
}

sub _build_queries {
	my $self = shift;
	my $group_key = shift;
	
#	unless($self->_get_index_schema($index)){
#		$self->add_warning(417, 'No index schema found for index ' . $index . ', not including in search', { index => $index });
#		return [];
#	}
	
	my @queries;
	# All OR's get factored out and become separate queries
	if (scalar keys %{ $self->terms->{or} }){
		foreach my $key (keys %{ $self->terms->{or} }){
			push @queries, @{ $self->_build_query($group_key, $key) };
		}
	}
	elsif (scalar keys %{ $self->terms->{and} }){
		push @queries, @{ $self->_build_query($group_key) };
	}
	else {
		throw(400, 'No positive value in query', { query => 0 });
	}
	
	return \@queries;
}

sub _build_query {
	my $self = shift;
	my $group_key = shift;
	my $or_key = shift;
	
	my $classes = $self->_get_class_ids($or_key);
	$self->log->debug('searching classes: ' . join(',', sort keys %$classes));
	my @queries;
	foreach my $class_id (sort keys %$classes){
		my $terms_and_filters = $self->_get_search_terms($class_id, $group_key, $or_key);
		$self->log->debug('terms_and_filters: ' . Dumper($terms_and_filters));
		my $match_str = $self->_get_match_str($class_id, $terms_and_filters->{searches}, $group_key);
		my $attr_str = $self->_get_attr_tests($class_id, $terms_and_filters->{filters});
		$self->log->debug('attr_str: ' . $attr_str);
		my $query = {
			select => $self->_get_select_clause($class_id, $attr_str),
			where => $self->_get_where_clause($class_id, $match_str),
			groupby => $self->_get_groupby_clause($class_id),
			orderby => $self->_get_orderby_clause($class_id),
		};
		$self->log->debug('query: ' . Dumper($query));
		push @queries, $query;
	}
	
	return \@queries;
}

sub _get_where_clause {
	my $self = shift;
	my $class_id = shift;
	my $match_str = shift;
	
	if ($class_id){
		return { 
			clause => 'MATCH(\'' . $match_str . '\') AND attr_tests=1 AND import_tests=1 AND class_id=?',
			values => [ $class_id ]
		}
	}
	else {
		return { 
			clause => 'MATCH(\'' . $match_str . '\') AND attr_tests=1 AND import_tests=1 ',
			values => []
		}
	}
}

sub _get_groupby_clause {
	my $self = shift;
	my $class_id = shift;
	
	return '' unless $self->groupby;
	return $self->_attr($self->groupby, $class_id);
}

sub _get_orderby_clause {
	my $self = shift;
	my $class_id = shift;
	
	return '' unless $self->orderby;
	return $self->_attr($self->orderby, $class_id);
}

# Divine what classes this query will encompass given the terms
sub _get_class_ids {
	my $self = shift;
	my $or_key = shift;
	
	my %classes;
	my $all_classes = 0;
	
	# If there is a groupby, verify that the classes in the groupby match the terms
	if ($self->groupby){
		if (defined $Fields::Field_to_order->{ $self->groupby } or defined $Fields::Reserved_fields->{ $self->groupby }){
			$all_classes = 1;
		}
		else {
			my $field_classes = $self->_classes_for_field($self->groupby);
			foreach my $class_id (keys %$field_classes){
				if ($self->user->is_permitted('class_id', $class_id)){
					$classes{$class_id} = $field_classes->{$class_id};
				}
			}
			unless (scalar keys %classes){
				throw(400, 'Field ' . $self->groupby . ' not a valid groupby value', { term => $self->groupby });
			}
		}
	}
	# If there is a orderby, verify that the classes in the orderby match the terms
	elsif ($self->orderby){
		if (defined $Fields::Field_to_order->{ $self->orderby } or defined $Fields::Reserved_fields->{ $self->orderby }){
			$all_classes = 1;
		}
		else {
			my $field_classes = $self->_classes_for_field($self->orderby);
			foreach my $class_id (keys %$field_classes){
				if ($self->user->is_permitted('class_id', $class_id)){
					$classes{$class_id} = $field_classes->{$class_id};
				}
			}
			unless (scalar keys %classes){
				throw(400, 'Field ' . $self->orderby . ' not a valid orderby value', { term => $self->orderby });
			}
		}
	}
	
	# Find any OR classes
	foreach my $key (keys %{ $self->terms->{or} }){
		if ($self->terms->{or}->{$key}->{field} eq 'class'){
			if ($self->meta_info->{classes}->{ $self->terms->{or}->{$key}->{value} }){
				my $class_name = $self->terms->{or}->{$key}->{value};
				my $class_id = $self->meta_info->{classes}->{ $self->terms->{or}->{$key}->{value} };
				$self->log->debug('including OR class: ' . $class_id);
				$classes{ $class_id } = $class_name;
			}
			else {
				throw(400, 'Invalid class ' . $self->terms->{or}->{$or_key}->{value}, { term => $self->terms->{or}->{$or_key}->{value} });
			}
		}
	}
	
	# Find the unique fields requested
	my %fields;
	if ($or_key and $self->terms->{or}->{$or_key}->{field}){
		 $fields{ $self->terms->{or}->{$or_key}->{field} } = $self->terms->{or}->{$or_key}->{value};
	}
	foreach my $boolean (qw(and not)){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			my $field = $self->terms->{$boolean}->{$key}->{field};
			next unless $field;
			my $value = $self->terms->{$boolean}->{$key}->{value};
			if ($field eq 'class'){
				if ($boolean eq 'and'){
					return { $self->meta_info->{classes}->{$value} => $value };
				}
			}
			else {
				$fields{$field} = $value;
			}
		}
	}
	
	# Foreach field, find classes
	foreach my $field (keys %fields){
		if ($self->_is_meta($field)){
			next;
		}
		my $field_classes = $self->_classes_for_field($field);
		$self->log->debug('classes for field ' . $field . ': ' . Dumper($field_classes));
		
		if (($self->groupby or $self->orderby) and not $all_classes){
			# Find the intersection of these classes
			my $same_counter = 0;
			foreach my $class_id (keys %$field_classes){
				$same_counter++ if exists $classes{$class_id};
			}
			
			if (not $same_counter){
				throw(400, 'Term ' . $field . ' is incompatible with groupby or orderby field', { term => $field });
			}
			else {
				foreach my $class_id (keys %classes){
					if (not exists $field_classes->{$class_id}){
						$self->log->debug('Removing class_id ' . $class_id . ' because field ' . $field . ' does not have it');
						delete $classes{$class_id};
					}
				}
			}
		}
		elsif (scalar keys %classes){
			# Find the intersection of these classes with the current list
			my @intersection = grep { exists $classes{$_} } keys %$field_classes;
			#$self->log->debug('intersection: ' . Dumper(\@intersection));
			%classes = ();
			@classes{ @intersection } = @{ $field_classes }{ @intersection };
			$self->log->debug('classes after intersection of field ' . $field . ': ' . Dumper(\%classes));
			# if there is no intersection return none
			if (not scalar keys %classes){
				return \%classes;
			}
		}
		else {
			%classes = %$field_classes;
		}
	}
	
	# Verify field permissions
	foreach my $boolean (qw(and or not)){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			next unless $self->terms->{$boolean}->{$key}->{field};
			foreach my $class_id (keys %classes){
				my $field = $self->terms->{$boolean}->{$key}->{field};
				if (not $self->get_field($field, $class_id) and $self->_attr($field, $class_id)){
					$field = $self->_attr($field, $class_id);
				}
				unless ($self->user->is_permitted($field, $self->terms->{$boolean}->{$key}->{value}, $class_id)){
					delete $classes{$class_id};
				}
			}
		}
	}
	
	$self->log->debug('classes: ' . Dumper(\%classes));
	
	# If no classes are specified via terms/groupby/orderby, go with all
	unless (scalar keys %classes){
		if ($self->user->permissions->{class_id}->{0}){
			return { 0 => 1 };
		}
		else {
			return $self->permitted_classes;
		}
	}
#	unless (scalar keys %fields or $self->groupby or $self->orderby){
#		if (not $self->user->permissions->{class_id}->{0}){
#			return $self->permitted_classes;
#		}
#	}
	
	return \%classes;
}

sub _search_value {
	my $self = shift;
	my $value = shift;
	my $group_key = shift;
	
	my $chars_indexed = (values %{ $self->schemas->{$group_key} })[0]->{schema}->{chars_indexed};
	
	my @value_chars = split(//, $value);
	my @output;
	foreach (@value_chars){
		if (exists $chars_indexed->{ lc($_) }){
			push @output, $_;
		}
		else {
			push @output, ' ';
		}
	}
	
	if ($value =~ /^["']/ and $value =~ /["']$/){
		return '"' . join('', @output) . '"';
	}
	else {
		# Escape any hyphens
		$value =~ s/\-/\\\\\-/g;
	}
	return join('', @output);
}
	

sub _get_match_str {
	my $self = shift;
	my $class_id = shift;
	my $searches = shift;
	my $group_key = shift;
	
	my %clauses;
	
	foreach my $hash (@{ $searches }){
		$clauses{ $hash->{boolean} } ||= {};
		
		if ($hash->{field}){
			my $value = $self->_is_int_field($hash->{field}, $class_id) ? $self->_value($hash, $class_id) : $hash->{value};
			$value = $self->_search_value($value, $group_key);
			my $field = $self->_search_field($hash->{field}, $class_id);
			$clauses{ $hash->{boolean} }->{'(@' . $field . ' ' . $value . ')'} = 1;
		}
		else {
			$clauses{ $hash->{boolean} }->{'(' . $hash->{value} . ')'} = 1;
		}
	}
	
	my @boolean_clauses;
	
	# The clauses need to be sorted such that terms with no field are first for Sphinx
	if (scalar keys %{ $clauses{and} }){
		push @boolean_clauses, '(' . join(' ', sort { $a =~ /^\(\@/ <=> $b =~ /^\(\@/ } keys %{ $clauses{and} }) . ')';
	}
	if (scalar keys %{ $clauses{or} }){
		push @boolean_clauses, '(' . join('|', sort { $a =~ /^\(\@/ <=> $b =~ /^\(\@/ } keys %{ $clauses{or} }) . ')';
	}
	if (scalar keys %{ $clauses{not} }){
		push @boolean_clauses, '!(' . join('|', sort { $a =~ /^\(\@/ <=> $b =~ /^\(\@/ } keys %{ $clauses{not} }) . ')';
	}
	
	my $match_str = join(' ', @boolean_clauses);
	
	$match_str =~ s/'/\\'/g;
	
	# Verify we'll still have something to search on
	my $chars_indexed = (values %{ $self->schemas->{$group_key} })[0]->{schema}->{chars_indexed};
	$chars_indexed = join('', keys %$chars_indexed);
	my $re = qr/[$chars_indexed]/i;
	unless ($match_str =~ $re){
		throw(400, 'Query did not contain any indexed characters.', { match_str => $match_str });
	}
	
	return $match_str;
}



sub _index_has {
	my $self = shift;
	my $index_schema = shift;
	my $type = shift;
	my $value = shift;
	
	return grep { $_ eq $value } @{ $index_schema->{$type} };
}

sub _is_stopword {
	my $self = shift;
	my $index_schema = shift;
	my $word = shift;
	
	return exists $index_schema->{stopwords}->{ lc($word) } or $self->parser->is_stopword($word);
}

sub _get_attr_tests {
	my $self = shift;
	my $class_id = shift;
	my $filters = shift;
	
	my %terms;
	foreach my $hash (@$filters){
		my $attr = $self->_attr($hash->{field}, $class_id);
		my $value = $self->_value($hash, $class_id);
		unless (defined $attr and defined $value){
			throw(400, 'Invalid filter ' . $hash->{field}, { term => $hash->{field} });
		}
		# Need to take special care on a negated range
		if ($hash->{op} =~ /[<>]/){
			$terms{'range_' . $hash->{boolean}} ||= {};
			$terms{'range_' . $hash->{boolean}}->{$attr} ||= [];
			push @{ $terms{'range_' . $hash->{boolean}}->{$attr} }, { value => $value, op => $hash->{op}, term => sprintf('%s%s%d', $attr, $hash->{op}, $value) };
		}
		else {
			push @{ $terms{ $hash->{boolean} } }, sprintf('%s%s%d', $attr, $hash->{op}, $value);
		}
	}
	
	my @attr_clauses;
	if ($terms{and} and scalar @{ $terms{and} }){
		push @attr_clauses, '(' . join(' AND ', @{ $terms{and} }) . ')';
	}
	if ($terms{or} and scalar @{ $terms{or} }){
		push @attr_clauses, '(' . join(' OR ', @{ $terms{or} }) . ')';
	}
	if ($terms{not} and scalar @{ $terms{not} }){
		push @attr_clauses, 'NOT (' . join(' OR ', @{ $terms{not} }) . ')';
	}
	foreach my $boolean (qw(and or not)){
		my $range_op = 'range_' . $boolean;
		if ($terms{$range_op} and scalar keys %{ $terms{$range_op} }){
			foreach my $attr (sort keys %{ $terms{$range_op} }){
				# need to pair up negated ranges with NOT (a AND B) OR NOT (c and d)
				my @sorted_terms = sort { $a->{value} <=> $b->{value} } @{ $terms{$range_op}->{$attr} };
				$self->log->debug('begin sorted: ' . Dumper(\@sorted_terms));
				# First check if there is an odd number, requiring an artifical floor or ceiling
				if (scalar @sorted_terms % 2 == 1){
					if ($sorted_terms[0]->{op} =~ /</){
						unshift @sorted_terms, { value => 0, op => '>=', term => sprintf('%s%s%d', $attr, '>=', 0) };
					}
					else {
						push @sorted_terms, { value => 2**32, op => '<=', term => sprintf('%s%s%d', $attr, '<=', 2**32) };
					}
				}
				$self->log->debug('end sorted: ' . Dumper(\@sorted_terms));
				for (my $i = 0; $i < @sorted_terms; $i += 2){
					push @attr_clauses, ($boolean eq 'not' ? 'NOT' : '') . ' (' . $sorted_terms[$i]->{term} . ' AND ' . $sorted_terms[$i + 1]->{term} . ')';
				}
			}
			$self->log->debug('attr_clauses: ' . Dumper(\@attr_clauses));
		}
	}
	
	return scalar @attr_clauses ? join(' AND ', @attr_clauses) : 1;
}

sub _get_select_clause {
	my $self = shift;
	my $class_id = shift;
	my $attr_string = shift;
	
	my $import_where = '1=1';
	
	if ($self->has_id_ranges > 100){
		throw(400, 'Query returned too many possible import sets', { count => $self->has_id_ranges });
	}
	
	if ($self->has_id_ranges){
		my %import_wheres = ( and => [], or => [], not => [] );
		foreach my $range ($self->all_id_ranges){
			push @{ $import_wheres{ $range->{boolean} } }, sprintf(' (%d<=id AND id<=%d)', @{ $range->{values} });
		}
		$import_where = '(1=1';
		if (scalar @{ $import_wheres{or} }){
			$import_where .= '(' . join(' OR ', @{ $import_wheres{or} }) . ')';
		}
		if (scalar @{ $import_wheres{and} }){
			$import_where .= join('' , map { ' AND ' . $_ } @{ $import_wheres{and} });
		}
		if (scalar @{ $import_wheres{not} }){
			$import_where .= join('', map { ' AND NOT ' . $_ } @{ $import_wheres{not} });
		}
		$import_where .= ')';
	}
	
	if ($self->groupby){
		return {
			clause => 'SELECT id, COUNT(*) AS _count, ' . $self->_attr($self->groupby, $class_id) . ' AS _groupby, '
			. $attr_string . ' AS attr_tests, ' . $import_where . ' AS import_tests',
			values => [],
		}
	}
	# Find our _orderby
	my $attr = $self->orderby ? $self->_attr($self->orderby, $class_id) : 'timestamp';
	return {
		clause => 'SELECT *, ' . $attr . ' AS _orderby, ' . $attr_string . ' AS attr_tests, ' . $import_where . ' AS import_tests',
		values => [],
	}
}

sub _get_search_terms {
	my $self = shift;
	my $class_id = shift;
	my $group_key = shift;
	my $or_key = shift;

	my $ret = { searches => [], filters => [] };
	
	# For some reason, the copy of the hash ref was optimizing to just a ref
	my $local_terms = { and => {}, not => {} };
	foreach my $boolean (qw(and not)){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			$local_terms->{$boolean}->{$key} = { %{ $self->terms->{$boolean}->{$key} } };
		}
	}
	if ($or_key){
		$self->log->debug('or_key: ' . $or_key);
		$local_terms->{and}->{$or_key} = $self->terms->{or}->{$or_key};
	}
	
	my $index_schema = $self->_get_index_schema($group_key);
	$self->log->debug('index_schema: ' . Dumper($index_schema));
	
	# Check for some basic problems, like impossible ANDs
	foreach my $key (keys %{ $local_terms->{and} }){
		my %hash = %{ $local_terms->{and}->{$key} };
		my @same_fields = grep { 
			$local_terms->{and}->{$_}->{field} eq $hash{field} 
				and $local_terms->{and}->{$_}->{value} ne $hash{value}
				and $local_terms->{and}->{$_}->{op} eq $hash{op} 
			} keys %{ $local_terms->{and} };
		foreach my $same_field_key (@same_fields){
			if ($self->_is_int_field($local_terms->{and}->{$same_field_key}->{field}, $class_id)){
				throw(400, 'Impossible query, conflicting terms: ' . join(', ', $key, @same_fields), { term => $local_terms->{and}->{$same_field_key}->{value} });
			}
		}
	}
	
	foreach my $boolean (qw(and not)){
		foreach my $key (keys %{ $local_terms->{$boolean} }){
			my %hash = %{ $local_terms->{$boolean}->{$key} };
			$hash{boolean} = $boolean;
			
			# Is this a non-search op?
			if ($hash{field} and not ($hash{op} eq ':' or $hash{op} eq '=' or $hash{op} eq '~')){
				push @{ $ret->{filters} }, { %hash };
				next;
			}
			
			# Is it an int field?
			if($hash{field} and $self->_is_int_field($hash{field}, $class_id)){
				$self->log->debug('field ' . $hash{field} . ' was an int field');
				push @{ $ret->{filters} }, { %hash };
				next;
			}
			
			# Is it quoted and the op isn't ~ ?
			elsif ($hash{field} and $hash{op} ne '~' and $hash{quote}){
				# Make it a filter
				push @{ $ret->{filters} }, { %hash };
				next;
			}
			
			# Is this a stopword?
			elsif ($self->_is_stopword($index_schema, $hash{value})){
				if ($self->groupby){
					throw(400, 'Query term ' . $hash{value} . ' is too common and cannot be used in a groupby', { term => $hash{value} });
				}
				# Will need to tack it on the post filter list
				$self->post_filters->{ $hash{boolean} }->{ $hash{value} } = $hash{field};
				next;
			}
			
			# Default to search term
			if (not $hash{field} or ($hash{field} and $self->_search_field($hash{field}, $class_id) 
				and $self->_index_has($index_schema, 'fields', $self->_search_field($hash{field}, $class_id)) ) ){
				push @{ $ret->{searches} }, { %hash };
			}
#			elsif($hash{field} and $self->_attr($hash{field}, $class_id) 
#				and $index_schema->{attrs}->{ $self->_attr($hash{field}, $class_id) }){
#				$self->log->warn('Making term ' . $hash{value} . ' into a filter because the field does not exist in this schema');
#				push @{ $ret->{filters} }, { %hash };
#			}
			else {
				throw(400, 'Unable to find field ' . $hash{field} . ' for class ' . $self->meta_info->{classes_by_id}->{$class_id}, { term => $hash{field} });
			}
		}
	}
	
	# Do we have any search terms now?
	if (grep { $_->{boolean} eq 'and' } @{ $ret->{searches} }){
		return $ret;
	}
	
	$self->log->debug('no search terms, finding candidates: ' . Dumper($ret));
	
	# else, we need to try to find a search term from the filters
	my %candidates;
	my %int_candidates;
	#$self->log->debug('index_schema: ' . Dumper($index_schema));
	foreach my $hash (@{ $ret->{filters} }){
		next unless $hash->{boolean} eq 'and';
		next if $self->_is_stopword($index_schema, $hash->{value});
		
		# Verify this field exists in this index
		my $field = $self->_search_field($hash->{field}, $class_id);
		my $attr = $self->_attr($hash->{field}, $class_id);
		$self->log->debug('attr: ' . $attr . ', field: ' . $field . ', search_field returns: ' . $self->_search_field($field, $class_id) .
			' search_field on attr returns: ' . $self->_search_field($attr, $class_id));
		if ($field and $self->_index_has($index_schema, 'fields', $self->_search_field($field, $class_id))){
			if ($self->_is_int_field($hash->{field}, $class_id) and $hash->{op} !~ /[<>]/){
				$int_candidates{ $hash->{value} } = $hash;
				$self->log->debug('attr ' . $attr . ' with value ' . $hash->{value} . ' is an int candidate');
			}
			else {
				$self->log->debug($field . ' with value ' . $hash->{value} . ' is a candidate');
				$candidates{ $hash->{value} } = $hash;
			}
		}
		elsif ($field and $self->_is_meta($field) or ($attr and $self->_is_meta($attr))){
			$self->log->debug('meta attr ' . $hash->{field} . ' not a candidate');
		}
		elsif ($field and $self->_index_has($index_schema, 'fields', $field)){
			$self->log->debug($field . ' with value ' . $hash->{value} . ' is a candidate');
			$candidates{ $hash->{value} } = $hash;
		}
		elsif ($attr and $self->_index_has($index_schema, 'attrs', $attr)){
			if ($self->_is_int_field($hash->{field}, $class_id) and $hash->{op} !~ /[<>]/){
				$int_candidates{ $hash->{value} } = $hash;
				$self->log->debug('attr ' . $attr . ' with value ' . $hash->{value} . ' is an int candidate');
			}
			else {
				$self->log->debug('attr ' . $attr . ' with value ' . $hash->{value} . ' is not an int candidate');
			}
		}
		else {
			$self->log->debug('index_schema: ' . Dumper($index_schema));
			throw(500, 'Unable to find field or attr for ' . $hash->{field}, { term => $hash->{field} });
		}
		next;
	}
	
	if (scalar keys %candidates){
		# Pick the longest
		my $longest = (sort { length($b) <=> length($a) } keys %candidates)[0];
		push @{ $ret->{searches} }, $candidates{$longest};
	}
	elsif (scalar keys %int_candidates){
		# Use an attribute as an anyfield query, pick the highest number after resolving to a value
		my $biggest = (sort { $self->_value($int_candidates{$b}, $class_id) <=> $self->_value($int_candidates{$a}, $class_id) } keys %int_candidates)[0];
		$self->log->debug('biggest value: ' . $biggest);
		if ($self->_is_meta($int_candidates{$biggest}->{field})){
			push @{ $ret->{searches} }, { field => '', value => $self->_value($int_candidates{$biggest}, $class_id), boolean => 'and' };
		}
		else {
			push @{ $ret->{searches} }, { field => '', value => $biggest, boolean => 'and' };
		}
	}
	
	# Final check to be sure we have positive search terms
	unless (scalar grep { $_->{boolean} eq 'and' } @{ $ret->{searches} }){
		throw(302, 'No search terms after checking fields and stopwords', { location => 'Query::SQL', directives => { use_sql_regex => 1 } });
	}
	
	
	return $ret;
}

sub _get_permissions_clause {
	my $self = shift;
}

sub _get_index_groups {
	my $self = shift;
	
	# Sort the indexes into groups by schema
	my %schemas;
	unless ($self->meta_info->{indexes} and $self->meta_info->{indexes}->{indexes}){
		return \%schemas;
	}
	foreach my $index (@{ $self->meta_info->{indexes}->{indexes} }){
		my $group_key = '';
		next unless $index->{schema} and scalar keys %{ $index->{schema} };
		foreach my $key (sort keys %{ $index->{schema} }){
			if (ref($index->{schema}->{$key}) and ref($index->{schema}->{$key}) eq 'HASH'){
				foreach my $schema_key (sort keys %{ $index->{schema}->{$key} }){
					$group_key .= $schema_key . ':' . $index->{schema}->{$key}->{$schema_key};
				}
			}
			elsif (ref($index->{schema}->{$key}) and ref($index->{schema}->{$key}) eq 'ARRAY'){
				foreach my $schema_key (@{ $index->{schema}->{$key} }){
					$group_key .= $schema_key;
				}
			}
			else {
				$group_key .= $key;
			}
		}
		$group_key = crc32($group_key);
		
		my $chars = $index->{schema}->{chars};
		my @terms = split(/\s*\,\s*/, $chars);
		my %chars_indexed;
		foreach (@terms){
			next if /\->/;
			if (/([^\.]+)\.\.([^\.]+)/){
				my ($start, $end) = ($1, $2);
				if ($start =~ /U\+(..)/){
					$start = hex($1);
				}
				else {
					$start = ord($start);
				}
				if ($end =~ /U\+(..)/){
					$end = hex($1);
				}
				else {
					$end = ord($end);
				}
				for ($start..$end){
					$chars_indexed{ chr($_) } = 1;
				}
			}
			else {
				if (/U\+(..)/){
					$chars_indexed{ chr(hex($1)) } = 1;
				}
				else {
					$chars_indexed{ $_ } = 1;
				}
			}
		}
		#$self->log->debug('chars_indexed: ' . Dumper(\%chars_indexed));
		$index->{schema}->{chars_indexed} = { %chars_indexed };
		
		$schemas{$group_key} ||= {};
		$schemas{$group_key}->{ $index->{name} } = $index;
	}
	
	return \%schemas;
}

sub _get_index_list {
	my $self = shift;
	my $indexes = shift;
	
	# Sort the indexes by time
	my $sort_fn;
	if ($self->orderby_dir eq 'DESC'){
		$sort_fn = sub { $indexes->{$b}->{start_int} <=> $indexes->{$a}->{start_int} };
	}
	else {
		$sort_fn = sub { $indexes->{$a}->{start_int} <=> $indexes->{$b}->{start_int} };
	}
	
	my @ret;
	my $start = defined $self->start ? $self->start : 0;
	my $end = defined $self->end ? $self->end : time;
	foreach my $index_name (sort $sort_fn keys %{ $indexes }){
		# Check that the time is right
		if (($indexes->{$index_name}->{start_int} <= $start and $indexes->{$index_name}->{end_int} >= $start)
			or ($indexes->{$index_name}->{start_int} <= $end and $indexes->{$index_name}->{end_int} >= $end)
			or ($indexes->{$index_name}->{start_int} >= $start and $indexes->{$index_name}->{end_int} <= $end)){
			push @ret, $index_name;
		}
	}
	
	return \@ret;
}

sub _get_index_schema {
	my $self = shift;
	my $group_key = shift;
	throw(500, 'Unable to find index ' . $group_key) unless $self->schemas->{$group_key};
	return (values %{ $self->schemas->{$group_key} })[0]->{schema};
#	foreach my $index (@{ $self->meta_info->{indexes}->{indexes} }){
#		if ($index->{name} eq $index_name){
#			return $index->{schema};
#		}
#	}
	
}
	

sub execute {
	my $self = shift;
	my $cb = shift;
	
	if ($self->parser->has_import_search_terms){
		$self->_find_import_ranges();
		if (not $self->has_id_ranges){
			$self->log->trace('Import terms eliminate all results');
			$cb->();
			return;
		}
	}
	
	$self->_get_data_db(sub {
		my $ok = shift;
		if (not $ok){
			$cb->($self->errors);
			return;
		}
		my $start = time();
		my $counter = 0;
		my $total = 0;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub {
			if (not $self->has_errors){
				if ($total){
					$self->results->percentage_complete(100 * $counter / $total);
				}
				else {
					$self->results->percentage_complete(100);
				}
				if ($self->results->total_records > $self->results->total_docs){
					$self->results->total_docs($self->results->total_records);
				}
			}
			$cb->();
			return;
		});
		
		unless (scalar keys %{ $self->schemas }){
			$self->add_warning(416, 'No data for time period queried', { term => 'start' });
			$cb->();
			return;
		}
		
		my $indexes_queried = 0;
		foreach my $group_key (keys %{ $self->schemas }){
			my $indexes = $self->_get_index_list($self->schemas->{$group_key});
			
			$self->log->trace('Querying indexes: ' . scalar @$indexes);
			$indexes_queried += scalar @$indexes;
			
			unless (scalar @$indexes){
				next;
			}
			
			last if $self->limit and $self->results->records_returned >= $self->limit;
			if ($self->timeout and (time() - $start) >= ($self->timeout/1000)){
				$self->results->is_approximate(1);
				last;
			}
			my $queries = $self->_build_queries($group_key);
			$total += scalar @$queries;
			$self->log->debug('$group_key: ' . Dumper($group_key));
			foreach my $query (@$queries){
				$cv->begin;
				$self->_query($indexes, $query, sub {
					my $per_index_results = shift;
					$counter++;
					if (not $per_index_results or $per_index_results->{error}){
						$self->log->error('Query error: ' . Dumper($per_index_results));
					}
					$cv->end;
				});
			}
			
		}
		
		unless ($indexes_queried){
			$self->add_warning(416, 'No data for time period queried', { term => 'start' });
			$cb->();
			return;
		}
		
		$cv->end;
	});
}

sub _query {
	my $self = shift;
	my $indexes = shift;
	my $query = shift;
	my $cb = shift;
	
	my $cv = AnyEvent->condvar;
	my $ret = { stats => {} };
	$cv->begin( sub{
		$self->log->debug('ret: ' . Dumper($ret));
		$cb->($ret);
	});
	
	#if (scalar @$indexes == scalar @{ $self->meta_info->{indexes}->{indexes} }){
	#	$indexes = ['distributed_local'];
	#}
	
	my @values = (@{ $query->{select}->{values} }, @{ $query->{where}->{values} });
	my $query_string = $query->{select}->{clause} . ' FROM ' . join(',', @$indexes) . ' WHERE ' . $query->{where}->{clause};
	if (defined $self->start){
		$query_string .= ' AND timestamp>=?';
		push @values, $self->start;
	}
	if (defined $self->end){
		$query_string .= ' AND timestamp<=?';
		push @values, $self->end;
	}
	if ($self->groupby){
		$query_string .= ' GROUP BY ' . $query->{groupby} . ' ORDER BY _count ';
	}
	elsif ($self->orderby){
		$query_string .= ' ORDER BY ' . $query->{orderby};
	}
	else {
		$query_string .= ' ORDER BY timestamp';
	}
	if ($self->orderby_dir eq 'DESC'){
		$query_string .= ' DESC';
	}
	else {
		$query_string .= ' ASC';
	}
	$query_string .= ' LIMIT ?,?';
	push @values, (0, $self->offset + $self->limit); # offset is enforced at the aggregation step, so we need to go offset + limit deep
	
	# Increase max_matches if limit > API default of 1000
	if ($self->limit > 1000){
		$query_string .= ' OPTION max_matches=' . int($self->limit);
	}
	
	$self->log->trace('Sphinx query: ' . $query_string);
	$self->log->trace('Sphinx query values: ' . join(',', @values));
	
	my $start = time();
	$self->data_db->{sphinx}->sphinx($query_string . ';SHOW META', 0, @values, sub {
		my ($dbh, $result, $rv) = @_;
		
		my $sphinx_query_time = (time() - $start);
		$self->log->debug('Sphinx query finished in ' . $sphinx_query_time);
		$ret->{stats}->{sphinx_query} += $sphinx_query_time;
		
		if (not $rv){
			my $e = 'sphinx got error ' .  Dumper($result);
			$self->log->error($e);
			$self->add_warning(500, $e, { sphinx => $self->peer_label });
			$ret = { error => $e };
			$cv->end;
			return;
		}
		my $rows = $result->{rows};
		$ret->{sphinx_rows} ||= [];
		push @{ $ret->{sphinx_rows} }, @$rows;
		$self->log->trace('got sphinx result: ' . Dumper($result));
		if ($ret->{meta}){
			foreach my $key (keys %{ $result->{meta} }){
				if ($key !~ /^keyword/ and $result->{meta}->{$key} =~ /^\d+(?:\.\d+)?$/){
					$ret->{meta}->{$key} += $result->{meta}->{$key};
				}
				else {
					$ret->{meta}->{$key} = $result->{meta}->{$key};
				}
			}
		}
		else {
			$ret->{meta} = $result->{meta};
		}
		
		# Go get the rows that contain the actual docs
		if (scalar @{ $ret->{sphinx_rows} }){
			$self->_get_rows($ret, sub {
				if ($self->groupby){
					$self->_format_records_groupby($ret);
				}
				else {
					$self->_format_records($ret);
					$self->_post_filter_results($ret);
				}
				$cv->end;
			});
		}
		else {
			$self->log->trace('No rows found');
			$cv->end;
		}
	});
}

# Intermediary method to abstract exactly how the real rows get retrieved
sub _get_rows {
	my $self = shift;
	my $ret = shift;
	my $cb = shift;
	
	my ($query, $sth);
	# Find what tables we need to query to resolve rows
	my %tables;
	
	ROW_LOOP: foreach my $row (@{ $ret->{sphinx_rows} }){
		foreach my $table_hash (@{ $self->meta_info->{tables}->{tables} }){
			next unless $table_hash->{table_type} eq 'index' or $table_hash->{table_type} eq 'import';
			if ($table_hash->{min_id} <= $row->{id} and $row->{id} <= $table_hash->{max_id}){
				$tables{ $table_hash->{table_name} } ||= [];
				push @{ $tables{ $table_hash->{table_name} } }, $row->{id};
				next ROW_LOOP;
			}
		}
	}
	
	unless (scalar keys %tables){
		$self->log->error('No tables found for results');
		$cb->($ret);
		return;
	}	
			
	# Go get the actual rows from the dbh
	my @table_queries;
	my @table_query_values;
	my %import_tables;
	foreach my $table (sort keys %tables){
		my $placeholders = join(',', map { '?' } @{ $tables{$table} });
		
		my $table_query = sprintf("SELECT %1\$s.id,\n" .
			"timestamp, host_id, program_id,\n" .
			"INET_NTOA(host_id) AS host, class_id, msg,\n" .
			"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
			"FROM %1\$s\n" .
			'WHERE %1$s.id IN (' . $placeholders . ')',
			$table, $self->data_db->{db});
		
		if ($table =~ /import/){
			$import_tables{$table} = $tables{$table};
		}
		push @table_queries, $table_query;
		push @table_query_values, @{ $tables{$table} };
	}
	
	if (keys %import_tables){
		$self->_get_import_rows(\%import_tables, $ret, sub {
			$self->_get_mysql_rows(\@table_queries, \@table_query_values, $ret, $cb);
		});
	}
	else {
		$self->_get_mysql_rows(\@table_queries, \@table_query_values, $ret, $cb);
	}
}

sub _get_extra_field_values {
	my $self = shift;
	my $ret = shift;
	my $cb = shift;
	
	my %programs;
	foreach my $row (values %{ $ret->{results} }){
		$programs{ $row->{program_id} } = $row->{program_id};
	}
	if (not scalar keys %programs){
		$cb->($ret);
		return;
	}
	
	my $query;
	$query = 'SELECT id, program FROM programs WHERE id IN (' . join(',', map { '?' } keys %programs) . ')';
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cb->($ret);
	});
	$self->data_db->{dbh}->query($query, (sort keys %programs), sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error getting extra field values ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cv->end;
			return;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get extra field value rows though we had values: ' . Dumper(\%programs)); 
		}
		else {
			$self->log->trace('got extra field value db rows: ' . (scalar @$rows));
			foreach my $row (@$rows){
				$programs{ $row->{id} } = $row->{program};
			}
			foreach my $id (keys %{ $ret->{results} }){
				$ret->{results}->{$id}->{program} = $programs{ $ret->{results}->{$id}->{program_id} };
			}
		}
		$cv->end;
	});
}

sub _format_records {
	my $self = shift;
	my $ret = shift;
	
	my @tmp;
	foreach my $id (sort { $a <=> $b } keys %{ $ret->{results} }){
		my $row = $ret->{results}->{$id};
		$row->{datasource} = 'Sphinx';
		$row->{_fields} = [
				{ field => 'host', value => $row->{host}, class => 'any' },
				{ field => 'program', value => $row->{program}, class => 'any' },
				{ field => 'class', value => $self->meta_info->{classes_by_id}->{ $row->{class_id} }, class => 'any' },
			];
		my $is_import = 0;
		foreach my $import_col (@{ $Fields::Import_fields }){
			if (exists $row->{$import_col}){
				$is_import++;
				push @{ $row->{_fields} }, { field => $import_col, value => $row->{$import_col}, class => 'any' };
			}
		}
		if ($is_import){					
			# Add node
			push @{ $row->{_fields} }, { field => 'node', value => $row->{node}, class => 'any' };
		}
		# Resolve column names for fields
		foreach my $col (qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5)){
			my $value = delete $row->{$col};
			# Swap the generic name with the specific field name for this class
			my $field = $self->meta_info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
			if (defined $value and $field){
				# See if we need to apply a conversion
				$value = $self->resolve_value($row->{class_id}, $value, $col);
				push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->meta_info->{classes_by_id}->{ $row->{class_id} } };
			}
		}
		push @tmp, $row;
	}
	
	
	# Now that we've got our results, order by our given order by
	if ($self->orderby_dir eq 'DESC'){
		foreach my $row (sort { $b->{_orderby} <=> $a->{_orderby} } @tmp){
			$self->results->add_result($row);
			last if $self->results->records_returned >= $self->limit;
		}
	}
	else {
		foreach my $row (sort { $a->{_orderby} <=> $b->{_orderby} } @tmp){
			$self->log->debug('adding row: ' . Dumper($row));
			$self->results->add_result($row);
			last if $self->results->records_returned >= $self->limit;
		}
	}
	
	$self->results->total_docs($self->results->total_docs + $ret->{meta}->{total_found});
}

sub _format_records_groupby {
	my $self = shift;
	my $ret = shift;
	
	my %agg;
	my $total_records = 0;
	
	# One-off for grouping by node
	if ($self->groupby eq 'node'){
		my $node_label = $self->peer_label ? $self->peer_label : '127.0.0.1';
		$agg{$node_label} = int($ret->{meta}->{total_found});
		next;
	}
	foreach my $id (sort { $a <=> $b } keys %{ $ret->{results} }){
		my $row = $ret->{results}->{$id};
		# Resolve the _groupby col with the mysql col
		unless (exists $ret->{results}->{ $row->{id} }){
			$self->log->warn('mysql row for sphinx id ' . $row->{id} . ' did not exist');
			next;
		}
		my $key;
		if (exists $Fields::Time_values->{ $self->groupby }){
			# We will resolve later
			$key = $row->{'_groupby'};
		}
		elsif ($self->groupby eq 'program'){
			$key = $ret->{results}->{ $row->{id} }->{program};
		}
		elsif ($self->groupby eq 'class'){
			$key = $self->meta_info->{classes_by_id}->{ $ret->{results}->{ $row->{id} }->{class_id} };
		}
		elsif (defined $Fields::Field_to_order->{ $self->groupby }){
			# Resolve normally
			$key = $self->resolve_value($row->{class_id}, $row->{'_groupby'}, $self->groupby);
		}
		else {
			# Resolve with the mysql row
			my $field_order = $self->get_field($self->groupby)->{ $row->{class_id} }->{field_order};
			#$self->log->trace('resolving with row ' . Dumper($ret->{results}->{ $row->{id} }));
			$key = $ret->{results}->{ $row->{id} }->{ $Fields::Field_order_to_field->{$field_order} };
			$key = $self->resolve_value($row->{class_id}, $key, $Fields::Field_order_to_field->{$field_order});
			$self->log->trace('field_order: ' . $field_order . ' key ' . $key);
		}
		$agg{ $key } += $row->{'_count'};	
	}
	
	if (exists $Fields::Time_values->{ $self->groupby }){
		# Sort these in ascending label order
		my @tmp;
		my $increment = $Fields::Time_values->{ $self->groupby };
		my $use_gmt = $increment >= 86400 ? 1 : 0;
		foreach my $key (sort { $a <=> $b } keys %agg){
			$total_records += $agg{$key};
			my $unixtime = $key * $increment;
			
			my $client_localtime = $unixtime - $self->parser->timezone_diff($unixtime);					
			$self->log->trace('key: ' . $key . ', tv: ' . $increment . 
				', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($client_localtime)));
			push @tmp, { 
				intval => $unixtime, 
				'_groupby' => epoch2iso($client_localtime, $use_gmt),
				'_count' => $agg{$key}
			};
		}
		
		# Fill in zeroes for missing data so the graph looks right
		my @zero_filled;
		
		$self->log->trace('using increment ' . $increment . ' for time value ' . $self->groupby);
		OUTER: for (my $i = 0; $i < @tmp; $i++){
			push @zero_filled, $tmp[$i];
			if (exists $tmp[$i+1]){
				for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
					push @zero_filled, { 
						'_groupby' => epoch2iso($j, $use_gmt),
						intval => $j,
						'_count' => 0
					};
					last OUTER if scalar @zero_filled >= $self->limit;
				}
			}
		}
		$self->results->add_results({ $self->groupby => [ @zero_filled ] });
	}
	else { 
		# Sort these in descending value order
		my @tmp;
		foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
			$total_records += $agg{$key};
			push @tmp, { intval => $agg{$key}, '_groupby' => $key, '_count' => $agg{$key} };
			last if scalar @tmp >= $self->limit;
		}
		$self->results->add_results({ $self->groupby => [ @tmp ] });
		$self->log->debug('@tmp: ' . Dumper(\@tmp));
	}
}

sub _post_filter_results {
	my $self = shift;
	my $ret = shift;
	
	if ($self->has_postfilters){
		$self->log->trace('post filtering results with: ' . Dumper($self->post_filters));
		my @keep = $self->results->all_results;
		my $removed = 0;
		for (my $i = 0; $i < @keep; $i++){
			if (not $self->_filter_stopwords($keep[$i])){
				splice(@keep, $i, 1);
				$removed++;
			}
		}
		$self->results->results([ @keep ]);
		$self->results->total_records($self->results->total_records - $removed);
	}
}

sub _filter_stopwords {
	my $self = shift;
	my $record = shift;
	
	# Filter any records which have stopwords
	if (scalar keys %{ $self->post_filters->{and} }){
		my $to_find = scalar keys %{ $self->post_filters->{and} };
		STOPWORD_LOOP: foreach my $stopword (keys %{ $self->post_filters->{and} }){
			my $regex = $self->term_to_regex($stopword);
			my $stopword_field = $self->post_filters->{and}->{$stopword};
			foreach my $field (keys %$record){
				if ((($stopword_field and $field eq $stopword_field) or not $stopword_field) and $record->{$field} =~ qr/$regex/i){
					$self->log->debug('Found stopword: ' . $stopword . ' for term ' . $record->{$field} . ' and field ' . $field);
					$to_find--;
					last STOPWORD_LOOP;
				}
			}
		}
		return 0 if $to_find;
	}
	
	if (scalar keys %{ $self->post_filters->{not} }){
		foreach my $stopword (keys %{ $self->post_filters->{not} }){
			my $regex = $self->term_to_regex($stopword);
			foreach my $field (keys %$record){
				if ($record->{$field} =~ qr/$regex/i){
					$self->log->debug('Found not stopword: ' . $stopword . ' for term ' . $record->{$field} . ' and field ' . $field);
					return 0;
				}
			}
		}
	}
	return 1;
}	
	
sub _get_import_rows {
	my $self = shift;
	my $import_tables = shift;
	my $ret = shift;
	my $cb = shift;
	
	my %import_info;
	my @import_queries;
	
	my $import_info_query = 'SELECT id AS import_id, name AS import_name, description AS import_description, ' .
		'datatype AS import_type, imported AS import_date, first_id, last_id FROM ' . $self->data_db->{db} 
		. '.imports WHERE ';
	my @import_info_query_clauses;
	my @import_query_values;
	foreach my $import_table (keys %{ $import_tables }){
		foreach my $id (@{ $import_tables->{$import_table} }){
			push @import_info_query_clauses, '? BETWEEN first_id AND last_id';
			push @import_query_values, $id;
		}
	}
	$import_info_query .= join(' OR ', @import_info_query_clauses);
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { $cb->($ret); });
	
	$self->data_db->{dbh}->query($import_info_query, @import_query_values, sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cv->end;
			return;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get import info rows though we had import values: ' . Dumper($import_tables)); 
		}
		$self->log->trace('got import info db rows: ' . (scalar @$rows));
		
		# Map each id to the right import info
		foreach my $table (sort keys %$import_tables){
			foreach my $id (@{ $import_tables->{$table} }){
				foreach my $row (@$rows){
					if ($row->{first_id} <= $id and $id <= $row->{last_id}){
						$import_info{$id} = $row;
						last;
					}
				}
			}
		}
		$self->log->debug('import_info: ' . Dumper(\%import_info));
		$ret->{import_info} = { %import_info };
		$cv->end;
	});
}		

sub _get_mysql_rows {
	my $self = shift;
	my $table_queries = shift;
	my $table_values = shift;
	my $ret = shift;
	my $cb = shift;
	
	# orderby_map preserves the _orderby field between Sphinx results and MySQL results
	my %orderby_map; 
	foreach my $row (@{ $ret->{sphinx_rows} }){
		if ($self->orderby){
			$orderby_map{ $row->{id} } = $row->{_orderby};
		}
	}
	$self->log->debug('%orderby_map  ' . Dumper(\%orderby_map));
	
	my $table_query = join(';', @$table_queries);
	$self->log->trace('table query: ' . $table_query . ', placeholders: ' . join(',', @$table_values));
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$self->_get_extra_field_values($ret, $cb);
	});
	
	my $start = time();
	
	$self->data_db->{dbh}->multi_query($table_query, @$table_values, sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error getting mysql rows ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { sphinx => $self->peer_label });
			$cv->end;
			return;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get rows though we had Sphinx results!'); 
		}
		$self->log->trace('got db rows: ' . (scalar @$rows));
		
		foreach my $row (@$rows){
			$ret->{results} ||= {};
			$row->{node} = $self->peer_label ? $self->peer_label : '127.0.0.1';
			$row->{node_id} = unpack('N*', inet_aton($row->{node}));
			if ($self->groupby){
				my ($sphinx_row) = grep { $_->{id} eq $row->{id} } @{ $ret->{sphinx_rows} };
				$row->{_groupby} = $sphinx_row->{_groupby};
				$row->{_count} = $sphinx_row->{_count};
			}
			if ($self->orderby){
				$row->{_orderby} = $orderby_map{ $row->{id} };
			}
			else {
				$row->{_orderby} = $row->{timestamp};
			}
			# Copy import info into the row
			if ($ret->{import_info} and exists $ret->{import_info}->{ $row->{id} }){
				foreach my $import_col (@{ $Fields::Import_fields }){
					if ($ret->{import_info}->{ $row->{id} }->{$import_col}){
						$row->{$import_col} = $ret->{import_info}->{ $row->{id} }->{$import_col};
					}
				}
			}
			$ret->{results}->{ $row->{id} } = $row;
		}
		$ret->{stats}->{mysql_query} += (time() - $start);
		$cv->end;
	});
}

sub _get_data_db {
	my $self = shift;
	my $cb = shift;
	my $conf = $self->conf->get('data_db');
	
	my $start = time();
	my $db_name = 'syslog';
	if ($conf->{db}){
		$db_name = $conf->{db};
	}
	
	my $mysql_port = 3306;
	if ($conf->{port}){
		$mysql_port = $conf->{port};
	}
			
	my $sphinx_port = 9306;
	if ($conf->{sphinx_port}){
		$sphinx_port = $conf->{sphinx_port};
	}
	my $ret = {};
	eval {
		$ret = { db => $db_name };
		$ret->{dbh} = SyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:database=' . $db_name . ';port=' . $mysql_port,  
			$conf->{username}, 
			$conf->{password}, 
			{
				mysql_connect_timeout => $self->db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
			}
		]);
		$self->log->trace('connecting to sphinx ');
		
		$ret->{sphinx} = SyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:port=' . $sphinx_port .';host=127.0.0.1', undef, undef,
			{
				mysql_connect_timeout => $self->db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
				mysql_bind_type_guessing => 1,
			}
		]);
	};
	if ($@){
		$self->add_warning(502, $@, { mysql => $self->peer_label });
		$cb->(0);
	}		
	
	$self->log->trace('All connected in ' . (time() - $start) . ' seconds');
	$self->data_db($ret);
	
	$cb->(1);
}

1;