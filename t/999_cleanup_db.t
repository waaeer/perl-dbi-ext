use strict;
use File::Path qw(remove_tree);
print "1..1\n";
my $pgconfig = $ENV{PGCONFIG} || 'pg_config';
open P, "$pgconfig --bindir |";
my $bindir = <P>; chomp($bindir);
if(! $> ) { 
	$< = $> = getpwnam('postgres');
}

my $dbdir = "/tmp/pgdata-$>";

system("$bindir/pg_ctl", "-D", $dbdir, '-m', 'immediate', 'stop');
remove_tree($dbdir);
print "ok 1\n";

