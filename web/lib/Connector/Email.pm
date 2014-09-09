package Connector::Email;
use Moose;
use Data::Dumper;
use MIME::Base64;
extends 'Connector';

our $Description = 'Send email';
sub description { return $Description }
sub admin_required { return 0 }

has 'query' => (is => 'rw', isa => 'Query', required => 1);

sub BUILD {
	my $self = shift;
	$self->controller->log->debug('got results to alert on: ' . Dumper($self->query->results));
		
	unless ($self->query->results->total_records){
		$self->controller->log->info('No results for query');
		return 0;
	}
	
	my @to = ($self->user->email);
	# Allow admin or none/local auth to override recipient
	if ($self->user->is_admin or $self->controller->conf->get('auth/none') or $self->controller->conf->get('auth/local')){
		if (scalar @{ $self->args }){
			@to = @{ $self->args };
		}
	}
	
	my $headers = {
		To => join(', ', @to),
		From => $self->controller->conf->get('email/display_address') ? $self->controller->conf->get('email/display_address') : 'system',
		Subject => $self->controller->conf->get('email/subject') ? $self->controller->conf->get('email/subject') : 'system',
	};
	my $body = sprintf('%d results for query %s', $self->query->results->records_returned, $self->query->query_string) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->controller->conf->get('email/base_url') ? $self->controller->conf->get('email/base_url') : 'http://localhost',
			$self->query->qid,
			$self->controller->_get_hash($self->query->qid),
	);
	if ($self->controller->conf->get('email/include_data')){
		if ($self->query->has_groupby){
			$body .= "\r\n" . $self->query->results->TO_JSON();
		}
		else {
			$body .= "\r\n";
			foreach my $row ($self->query->results->all_results){
				$body .= $row->{msg} . "\r\n";
			}
		}	
	}
	my ($query, $sth);
	$query = 'SELECT UNIX_TIMESTAMP(last_alert) AS last_alert, alert_threshold FROM query_schedule WHERE id=?';
	$sth = $self->controller->db->prepare($query);
	$sth->execute($self->query->schedule_id);
	my $row = $sth->fetchrow_hashref;
	if ((time() - $row->{last_alert}) < $row->{alert_threshold}){
		$self->controller->log->warn('Not alerting because last alert was at ' . (scalar localtime($row->{last_alert})) 
			. ' and threshold is at ' . $row->{alert_threshold} . ' seconds.' );
		return;
	}
	else {
		$query = 'UPDATE query_schedule SET last_alert=NOW() WHERE id=?';
		$sth = $self->controller->db->prepare($query);
		$sth->execute($self->query->schedule_id);
	}
	
	$self->controller->send_email({ headers => $headers, body => $body, user => 'system'}, sub {});
	
	# Check to see if we saved the results previously
	$query = 'SELECT qid FROM saved_results WHERE qid=?';
	$sth = $self->controller->db->prepare($query);
	$sth->execute($self->query->qid);
	$row = $sth->fetchrow_hashref;
	unless ($row){
		# Save the results
		$self->query->comments('Scheduled Query ' . $self->query->schedule_id);
		$self->controller->save_results($self->query->TO_JSON, sub {});
	}
}

1