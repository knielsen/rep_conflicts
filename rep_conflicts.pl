#! /usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use List::Util qw(uniq);
use Time::Local qw(timelocal);

my $opt_retries_file;
my $opt_help;
my $opt_verbose;
my $opt_interval = 10;
my $opt_table_limit = 5;

my $gtids = {};
my $hist = {};
my $max_seq_no = {};
my $start_time_bin;

GetOptions
    ("help|h" => \$opt_help,
     "slave-retries-file|r=s" => \$opt_retries_file,
     "interval|i=i" => \$opt_interval,
     "num-tables|n=i" => \$opt_table_limit,
     "verbose|v" => \$opt_verbose)
    or usage();

usage() if $opt_help;

read_binlog(@ARGV);

if (defined($opt_retries_file)) {
  open R, '<', $opt_retries_file
      or die "Failed to open file '$opt_retries_file' for reading: $!\n";
  seek_retries(\*R);
  scan_retries(\*R);
  close R;

  show_hist();
}
exit 0;


# Calculate a hash key for the time interval in which given timestamp lies.
sub calc_time_bin {
  my ($yr, $mo, $da, $hr, $mi, $sc) = @_;
  my $day_sec = $hr*3600 + $mi * 60 + $sc;
  my $day = timelocal(0, 0, 0, $da, $mo - 1, $yr - 1900);
  my @x = localtime($day + $day_sec - ($day_sec % $opt_interval));
  my $time_bin = sprintf '%04d-%02d-%02d %02d:%02d:%02d',
      $x[5] + 1900, $x[4] + 1, $x[3], $x[2], $x[1], $x[0];
  return $time_bin;
}


# Show the histogram of conflicts, as computed from the --log-slave-retries file.
sub show_hist {
  print "\n";
  print "Conflicts/retries per $opt_interval seconds interval, total and per tables:\n\n";
  for my $k (sort keys %$hist) {
    my @elems = map "$hist->{$k}[3]{$_}:[$_]",
        (sort { $hist->{$k}[3]{$b} <=> $hist->{$k}[3]{$a} } keys %{$hist->{$k}[3]});
    @elems= splice(@elems, 0, $opt_table_limit)
        if ($opt_table_limit);
    # Try to estimate the actual number of transactions per time interval on
    # the slave from the GTID sequence numbers (the binlog timestamps are the
    # master's execution time). This will be an approximation as we only see
    # timestamps for retried GTIDs.
    # Note that this currently assumes all/most GTIDs are using domain 0.
    # Otherwise a more elaborate counting method would be needed.
    my $prev_seq_no= $hist->{$k}[1];
    my $end_seq_no= $hist->{$k}[2];
    my $conflicts= $hist->{$k}[0];
    my $pct_conflicts;
    if (defined($prev_seq_no) && defined($end_seq_no) &&
        $end_seq_no > $prev_seq_no) {
      $pct_conflicts =
          sprintf("~%4.1f%%", $conflicts / ($end_seq_no - $prev_seq_no) * 100);
    } else {
      $pct_conflicts = '  ???%';
    }
    printf "%s: %5d (%s) (%s)\n", $k, $conflicts, $pct_conflicts, join(" ", @elems);
  }
}


# Read replication retries from the --log-slave-retries file, and compute the
# histogram of conflicts per interval.
sub scan_retries {
  my ($fh) = @_;
  my $last_bin = undef;
  my $prev_seq_no;
  while (<$fh>) {
    if (m/^([0-9]{4})-([0-9]{2})-([0-9]{2})  ?([0-9]{1,2}):([0-9]{2}):([0-9]{2}).*\[SUCCESS\].*GTID: ([0-9]+-[0-9]+-[0-9]+)/) {
      my ($y,$mo,$d,$h, $mi, $s, $gtid)= ($1, $2, $3, $4, $5, $6, $7);
      my $time_bin = calc_time_bin($1, $2, $3, $4, $5, $6);
      next if (defined($start_time_bin) && ($time_bin cmp $start_time_bin) < 0);
      $gtid =~ m/([0-9]+)-[0-9]+-([0-9]+)/ or die "Internal: $gtid";
      my ($domain, $seq_no)= ($1, $2);
      my $sub_key = '?';
      if (exists($gtids->{$gtid})) {
        my $tbls= $gtids->{$gtid}{TABLES};
        if (scalar(@$tbls)) {
          $sub_key = join(',', uniq sort @$tbls);
        } else {
          if ($opt_verbose) {
            print STDERR "Warning: No table name found for GTID $gtid\n";
          }
        }
      }
      my $e;
      if (exists($hist->{$time_bin})) {
        $e= $hist->{$time_bin};
        $e->[1]= $prev_seq_no
            unless defined($e->[1]);
        $e->[2]= $seq_no
            if $domain == 0;
      } else {
        last if $last_bin;
        $e = $hist->{$time_bin} = [ 0,
                                    $prev_seq_no,
                                    ($domain == 0 ? $seq_no : $prev_seq_no),
                                    { }];
      }
      ++$hist->{$time_bin}[0];
      if (exists($e->[3]{$sub_key})) {
        ++$e->[3]{$sub_key};
      } else {
        $e->[3]{$sub_key} = 1;
      }

      $prev_seq_no= $seq_no
          if $domain == 0;
      # Stop reading the potentially huge retries file when we reach a GTID
      # that's after the last GTID in the binlog.
      $last_bin = (exists($max_seq_no->{$domain}) && $max_seq_no->{$domain} < $seq_no);
    }
  }
}


# Binary seek to the time in the retries file where our first GTID was
# replicated. Used to more quickly find the interesting lines in a huge file.
sub seek_retries {
  my ($fh) = @_;
  my $gran = 16384;
  my $a = 0;
  seek $fh, 0, 2
      or die "I/O error on retries file: $!\n";
  my $file_end = tell $fh;
  my $b = int(($file_end + $gran - 1) / $gran);

  while (defined($start_time_bin) && $b > $a + 1) {
    my $c = int(($a + $b) / 2);
    seek $fh, $c*$gran, 0
        or die "I/O error on retries file: $!\n";
    my $bin;
    while (<$fh>) {
      next unless (m/^([0-9]{4})-([0-9]{2})-([0-9]{2})  ?([0-9]{1,2}):([0-9]{2}):([0-9]{2})/);
      $bin = calc_time_bin($1, $2, $3, $4, $5, $6);
      last;
    }
    if (defined($bin) && ($bin cmp $start_time_bin) < 0) {
      $a = $c;
    } else {
      $b = $c;
    }
  }
  seek $fh, $a*$gran, 0
      or die "I/O error on retries file: $!\n";
}


# Read GTIDs and involved table names from a binlog file (in text format as
# output by mysqlbinlog).
sub read_binlog {
  print "\nTransactions per $opt_interval seconds interval, as executed on *master*:\n\n"
      if scalar(@_) > 0;
  my $cur_gtid;
  my $cur_bin;
  my $gtid_count = 0;
  for my $binlog_name (@_) {
    open B, '<', $binlog_name
        or die "Failed to open '$binlog_name' for reading: $!\n";
    # Let's protect against a common mistake on running on the raw binlog file
    # and not the output of mysqlbinlog.
    # ToDo: Would be nice to call mysqlbinlog ourselves here.
    my $magic;
    my $res= read(B, $magic, 4);
    die "Error reading binlog file '$binlog_name'"
        unless defined($res);
    die "Error: File '$binlog_name' appears to be a raw binlog file; this program must be run on the output of mysqlbinlog redirected to a file.\n"
        if $magic eq "\xfebin";
    while (<B>) {
      if (m/^#([0-9]{6})  ?([0-9]+):([0-9]+):([0-9]+) server id.*\sGTID ([0-9]+-[0-9]+-[0-9]+)/) {
        $cur_gtid= $5;
        $gtids->{$cur_gtid}= { STAMP => [$1, $2, $3, $4], TABLES => [] };
        my $time_bin =
            calc_time_bin(substr($1, 0, 2) + 2000, substr($1, 2, 2),
                          substr($1, 4, 2), $2, $3, $4);
        $start_time_bin = $time_bin
            if !defined($start_time_bin) || ($time_bin cmp $start_time_bin) < 0;
        if (defined($cur_bin)) {
          if (($time_bin cmp $cur_bin) > 0) {
            printf "%s: %7d (%8.1f/s)\n", $cur_bin, $gtid_count, $gtid_count/$opt_interval;
            $cur_bin = $time_bin;
            $gtid_count = 0;
          }
        } else {
          $cur_bin = $time_bin;
        }
        ++$gtid_count;
        $cur_gtid =~ m/([0-9]+)-[0-9]+-([0-9]+)/ or die "Internal: $cur_gtid";
        $max_seq_no->{$1} = $2
            if (!exists($max_seq_no->{$1}) || $max_seq_no->{$1} < $2);
      } elsif (m/(INSERT(\s+INTO)?|DELETE(\s+FROM)?|UPDATE)\s+([^\s]{1,50})/) {
        push @{$gtids->{$cur_gtid}{TABLES}}, $4;
      }
    }
    close B;
  }
  if ($gtid_count > 0) {
    printf "%s: %7d (%8.1f/s)\n", $cur_bin, $gtid_count, $gtid_count/$opt_interval;
  }
}


sub usage {
  print STDERR <<"END";
Usage: $0 [<options>] [--slave-retries-file=<file>] binlog-NNNNNN.txt ...

Shows number of conflicts and transactions during replication of a set of
binlogs. The binlogs are specified as filenames as output from mysqlbinlog.
The date is grouped in intervals as specified by the --interval option.

If --slave-retries-file is specified, will read the file output from the
server --log-slave-retries option and show replication conflicts
cross-reference with GTIDs in the binlog.

Note that transactions per second is as executed on the *master*, not
as replicated on slave.

Options:
  --slave-retries-file=FILE, -r FILE
    Read the file written by the server from --log-slave-retries=FILE.

  --interval=N, -i N
    Group data into intervals of N seconds (default: $opt_interval).

  --num-tables=N, -n N
    Limit conflict frequencies to max. N table combinations (default: $opt_table_limit).

  --verbose, -v
    Verbose operation; shows GTIDs where table name could not be determined.

  --help, -h
    Show this usage description.
END
 exit 1;
}
