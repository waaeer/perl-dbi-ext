use strict;
use File::Path qw(remove_tree);
print "1..2\n";

my $pgconfig = $ENV{PGCONFIG} || 'pg_config';
open (my $p,"$pgconfig --bindir|") || die("Cannot open pipe from $pgconfig: $!");

my $bindir = <$p>; chomp($bindir);
close $p;

my $dbdir = "/tmp/pgdata-$<";

if( -f "$dbdir/postmaster.pid") { 
	system("$bindir/pg_ctl", "-D", $dbdir, '-m', 'immediate', 'stop');
}

if( -d $dbdir ) { 
	remove_tree($dbdir);
}

warn "Test pgdata=$dbdir\n";
mkdir ($dbdir, 0700) || die("Cannot make dir $dbdir : $!");
system("$bindir/initdb", "-D", $dbdir) && die("Cannot initdb ($bindir/initdb): $!");

my $port = 10900; ## toDo: select a port
open(C,'>>', "$dbdir/postgresql.conf") || die("Cannot append to $dbdir/postgresql.conf : $!");
print C "port = $port \n unix_socket_directories = '/tmp'\n";
close C;
warn "STARTING \n";
print "ok 1\n";
my $pid = fork();
if(!$pid) { 
	system("$bindir/pg_ctl", "-D", $dbdir, '-l', "/tmp/pglog-$<", 'start') && die("Cannot start postgres: $!");
	print "ok 2\n";
	exit(0);
}
wait();
warn "Done\n";






