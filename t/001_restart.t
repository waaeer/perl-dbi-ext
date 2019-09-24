use strict;
use DBI::Ext;
print "1..5\n";
my $pgconfig = $ENV{PGCONFIG} || 'pg_config';
open P, "$pgconfig --bindir |";
my $bindir = <P>; chomp($bindir);
my $port = 10900; ## toDo: selected port
my $dbdir = "/tmp/pgdata-$<";


my $cmd = "select count(*) from pg_class;";
my $dbi = DBI::Ext->new(dsn=>"dbi:Pg:dbname=postgres;host=/tmp;port=$port", reconnect_on_error=>1);
$dbi->connect_later;
my $n;
my $n_good_results;
for my $i (0..1) {
	my $res = $dbi->selectrow_arrayref($cmd)->[0];
	$n ||= $res;
}

system("$bindir/pg_ctl", "-D", $dbdir, '-l', "/tmp/pglog-$<", 'restart') && die("Cannot restart postgres: $!");

for my $i (0..2) {
	sleep(1);
	my $res = eval { $dbi->selectrow_arrayref($cmd)->[0]; };
	if($res == $n) { $n_good_results++; }
}

system("$bindir/pg_ctl", "-D", $dbdir, '-l', "/tmp/pglog-$<", 'restart') && die("Cannot restart postgres: $!");

for my $i (0..2) {
	sleep(1);
	my $res = eval { $dbi->selectrow_arrayref($cmd)->[0]; };
	if($res == $n) { $n_good_results++; }
}

if($n_good_results == 4) { 
	print "ok 1\n";
} else { 
	print "not ok 1\n";
}

eval { $dbi->do('create table t (x int)'); };

$dbi->begin_work();
$dbi->do('insert into t values (1)');
$dbi->commit();

my $t = $dbi->selectrow_arrayref('select sum(x) from t')->[0];
if($t == 1) {
	print "ok 2\n";
} else { 
	print "not ok 2\n";
}

$dbi->begin_work();
  $dbi->do('insert into t values (2)');
  $dbi->begin_work();
	$dbi->do('insert into t values (4)');
  $dbi->rollback();
  $dbi->do('insert into t values (8)');
  $dbi->begin_work();
	$dbi->do('insert into t values (16)');
  $dbi->commit();
$dbi->commit();

$t = $dbi->selectrow_arrayref('select sum(x) from t')->[0];
if($t == 27) {
	print "ok 3\n";
} else { 
	print "not ok 3\n";
	warn "t=$t\n";
}

$dbi->do('delete from t');

# старт транзакции без коннекта
$dbi->disconnect;
$dbi->connect_later;
$dbi->begin_work();
$dbi->do('insert into t values (1)');
$dbi->rollback();

$t = $dbi->selectrow_arrayref('select sum(x) from t')->[0];
if($t == 0) {
	print "ok 4\n";
} else { 
	print "not ok 4\n";
	warn "t=$t\n";
}

# старт транзакции после падения базы

system("$bindir/pg_ctl", "-D", $dbdir, '-l', "/tmp/pglog-$<", 'restart') && die("Cannot restart postgres: $!");

$dbi->begin_work();
warn "BEGAN\n";
$dbi->do('insert into t values (1)');
warn "INSERTED\n";
$dbi->rollback();

$t = $dbi->selectrow_arrayref('select sum(x) from t')->[0];
if($t == 0) {
	print "ok 5\n";
} else { 
	print "not ok 5\n";
	warn "t=$t\n";
}
warn "END\n";




















