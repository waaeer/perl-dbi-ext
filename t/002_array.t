use strict;
use DBI::Ext;
use Data::Dumper;

print "1..4\n";
my $pgconfig = $ENV{PGCONFIG} || 'pg_config';
open P, "$pgconfig --bindir |";
my $bindir = <P>; chomp($bindir);
my $port = 10900; ## toDo: selected port
if(! $> ) { 
	$< = $> = getpwnam('postgres');
}

my $dbdir = "/tmp/pgdata-$>";


my $dbi = DBI::Ext->new(dsn=>"dbi:Pg:dbname=postgres;host=/tmp;port=$port", reconnect_on_error=>1);

my $r1  = $dbi->selectrow_arrayref("select ARRAY[1,2,3] as x")->[0];
if (ref($r1) eq 'ARRAY' && join('',@$r1) eq '123') { 
	print "ok 1\n";
}  
my $r2  = $dbi->selectrow_arrayref("select ARRAY[1,2,3]::int8[] as x")->[0];
if (ref($r2) eq 'ARRAY' && join('',@$r2) eq '123') { 
	print "ok 2\n";
}
eval { $dbi->do('CREATE DOMAIN idtype int4'); };
my $r3  = $dbi->selectrow_arrayref("select ARRAY[1,2,3]::idtype[] as x")->[0];
if (!ref($r3) && $r3 eq '{1,2,3}') { 
	print "ok 3\n";   ## this is bad feature of dbd::pg :(:(
}



my $r4  = $dbi->selectrow_arrayref("select ARRAY['1','2','3']::text[] as x")->[0];
if (ref($r4) eq 'ARRAY' && join('',@$r4) eq '123') { 
	print "ok 4\n";
}


warn Data::Dumper::Dumper($r1,$r2,$r3, $r4);




