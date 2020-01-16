package DBI::Ext;
use strict;
use DBI qw(:sql_types);
use Time::HiRes;
use vars qw($VERSION $AUTOLOAD);
$VERSION = '0.1';
use Carp;

sub new { #
	my ($class, %opt) = @_;
	my $self = bless {%opt}, $class;
	$self->_init();
	return $self;
}

sub _init { 
	my $self = shift;
	$self->{retries}            //= 2;
	$self->{reconnect_wait}     ||= 1;
	$self->{reconnect_on_error} //= 1;
	$self->{attr} ||= {};
	$self->reconnect unless $self->{connect_later};
}

sub connect_later { 
	my $self = shift;
	$self->{connect_later} = 1;
}

sub disconnect { 
	my $self = shift;
	if ($self->{DBH} && ref($self->{DBH}) eq 'DBI::db') {
		$self->{DBH}->disconnect;
	}
	undef $self->{DBH};
}

sub connect {
	my $self = shift;
	my $dbh = $self->{DBH} =  DBI->connect(@{$self}{qw (dsn user password)},{%{$self->{attr}}, PrintError => 0, RaiseError => 0, AutoCommit => 1 });
	if($dbh) {
		$self->{connect_later} = 0;
		$self->{connection_lost} = 0;
		$self->{in_transaction}  = 0;
		&{$self->{postconnect}}($self) if($self->{postconnect}); 
		return 1;
	} else {
		warn "Connect error: ".$DBI::errstr;
		return 0; 
	}
}

sub reconnect {
	my $self = shift;
	if ($self->{DBH}) { 
		if( $self->{DBH}->ping) { 
			return 1; #success
		} else { 
			$self->disconnect;  # connection exists, but not working
		}
	}
	my $attempt = 0;
	while(1) { 
		if($self->connect()) { return 1; } # success
		$attempt ++;
		return 0 if $attempt >= $self->{retries};
		Time::HiRes::sleep($self->{reconnect_wait});
	}
	return 0; # failure
}

sub DESTROY { 
	my $self = shift;
	$self->{DBH}->disconnect if $self->{DBH};
	undef $self->{DBH};
}

sub AUTOLOAD { 
	my @args = @_;
	my $self = shift;

	no strict 'refs';
	my $al_func = $AUTOLOAD;
	$al_func =~ s/^.*DBI::Ext:://;

	my $func = sub { 
		my ($self, @opt) = @_;
		if(($self->{connect_later} || $self->{reconnect_on_error}) && !$self->{in_transaction} && ! $self->{DBH}) {
			$self->reconnect;
		}
		if(!$self->{DBH}) { 
			die("Database $self->{dsn} not connected");
		}
		my $ret = $self->{DBH}->$al_func(@opt); 
		if($self->{DBH}->state) { 
			$self->process_error();
		}
		return $ret;
	};
	*{$AUTOLOAD} = $func;
	return $func->(@args);
}

sub select_hashes { 
# toDo	
	

}

sub process_error {
	my $self = shift;
	my %opt  = @_;
	my $sqlstate = $self->{DBH} ? $self->{DBH}->state : '0888';			
	my $err      = $self->{DBH} ? $self->{DBH}->errstr : 'No connection';
#	warn "SQLSTATE=$sqlstate; err=$err\n";
	if($sqlstate =~ /^08|^57P01/) { # connection problems | terminated connection due to administrator command
		$self->{connection_lost} = 1;
		$self->{in_transaction}  = 0;
		undef $self->{DBH};     # will cause remy $dbdir = "/tmp/pgdata-$<";connect next time
		if($opt{retry}) { 
			if($self->reconnect()) {
				eval { $opt{retry}->(); } 
				or $self->process_error();
			}
		}
	
	} elsif($self->{in_transaction}) {
		$self->rollback;
		Carp::cluck($err);
		die bless [$err], 'DBI::Ext::RolledBackError';	
	}
	Carp::cluck($err);
	die $err;
}
sub begin_subtransaction { 
	my $self = shift;
	if(!$self->{in_transaction}) { 
		die("Cannot begin_subtransaction: not in transaction");
	}
	$self->{in_transaction}++;
	my $savepoint = ++$self->{_savepoint};
	push @{$self->{_savepoints}}, $savepoint;
	my $ok  = $self->{DBH}->do(sprintf('SAVEPOINT s%d', $savepoint),{RaiseError=>0});
	if(!$ok) {
		$self->process_error();
	}
}
sub commit_subtransaction { 
	my $self = shift;
	if($self->{in_transaction} <=1 ) { 
		die("Cannot commit_subtransaction: not in a subtransaction");
	}
	my $savepoint = pop @{$self->{_savepoints}};
	$self->{in_transaction}--;
	my $ok = $self->{DBH}->do(sprintf('RELEASE SAVEPOINT s%d', $savepoint),{RaiseError=>0});
	if(!$ok) { 
		$self->process_error();
	}
}
sub rollback_subtransaction { 
	my $self = shift;
	if($self->{in_transaction} <=1 ) { 
		die("Cannot commit_subtransaction: not in a subtransaction");
	}

	my $savepoint = pop @{$self->{_savepoints}};
	$self->{in_transaction}--;
	my $ok = $self->{DBH}->do(sprintf('ROLLBACK TO SAVEPOINT s%d', $savepoint),{RaiseError=>0});
	if(!$ok) { 
		$self->process_error();
	}
}

sub begin_work { 
	my $self = shift;
	if($self->{in_transaction}) { 
		return $self->begin_subtransaction();
	} 
	if($self->{connect_later} || $self->{reconnect_on_error}) {
		$self->reconnect;
	}
#	$self->{DBH}->{AutoCommit} = 0;
	$self->{DBH}->begin_work();
	$self->{_savepoint} = 0;
	$self->{_savepoints} = [];
	$self->{in_transaction} = 1;
	
}

sub commit { 
	my $self = shift;
	if($self->{in_transaction}>1) { 
		return $self->commit_subtransaction();
	}
	my $rc = eval { $self->{DBH}->commit() };
	if(!$rc) { 
		$self->process_error();
	}
	$self->{in_transaction} = 0;
	$self->{DBH}->{AutoCommit} = 1;
} 

sub rollback { 
	my $self = shift;
	if($self->{in_transaction} > 1) { 
			$self->rollback_subtransaction;
	} else { 
		if(eval{$self->{DBH}->rollback}) { 
			$self->{in_transaction} = 0;
			$self->{DBH}->{AutoCommit} = 1;
		} else { 
			my $newerr = $self->{DBH}->errstr;
			warn "Panic: cannot rollback: $newerr";
			$self->disconnect;
		}
	}
}

sub select_hashes {
	my ($self, $sql, @sqlopt) = @_;
	my $sth = $self->prepare($sql);
	my $rv = $sth->execute(@sqlopt);
	my @rows; 
    while (my $r = $sth->fetchrow_hashref) {
		push @rows, $r;
		# Unfortunately, DBD::Pg expands only arrays of harcoded types. And never expands JSON's
		foreach my $k (keys %$r) { 
				# временно - затычка (надо патчить DBD::Pg)
				# надо разобраться с JSON(B) и с массивами неизвестных типов
		}
	}
    $sth->finish;
	return \@rows;
}

sub transaction { 
	my ($self, $func, %opt) = @_;

	my $ret = eval { $self->begin_work; my $ret = &$func; $self->commit; return $ret; };
	if (my $err = $@) { 
		if(ref($err) && $err->isa('DBI::Ext::RolledBackError')) { 
			die $err->[0];
		} else { 
			$self->rollback;
			die $err;
		}
	}
	return $ret;
}

1;

=pod

features:
1) reconnect
2) transaction
3) select_hashes with expanding json and arrays in 



=cut
