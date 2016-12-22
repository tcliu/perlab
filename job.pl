#!/usr/bin/perl

use strict;
use warnings;
use Cwd qw(abs_path);
use List::Util qw(min max);
use File::Path qw(make_path);

my $arg_count = $#ARGV + 1;
if ($arg_count < 4) {
  die "Usage: $0 <job_id> <start_date> <end_date> <interval> [params]\n";
}

my ($job_id, $start_date, $end_date, $interval, @params) = @ARGV;

my $root_path = abs_path();
my $vm_args = "-Dkey=value";
my $jar_file = "$root_path/my.jar";
my $cmd_prefix = "nohup java $vm_args -jar $jar_file";
my $start_date_value = date_value($start_date);
my $end_date_value = date_value($end_date);
my $next_date_value;

for (my $cur_date_value = $start_date_value; ($cur_date_value - $end_date_value) * ($interval > 0 ? 1 : -1) < 0; $cur_date_value = $next_date_value) {
  $next_date_value = bound_value($cur_date_value + $interval, $start_date_value, $end_date_value);
  my @cur_date = get_date($cur_date_value);
  my @next_date = get_date($next_date_value);
  my @from_date = $cur_date_value < $next_date_value ? @cur_date : @next_date;
  my @to_date = $cur_date_value < $next_date_value ? @next_date : @cur_date;
  process_job(\@from_date, \@to_date);
}

sub process_job {
  my @from_date = @{$_[0]};
  my @to_date = @{$_[1]};
  my $from_date_str = date_str(@from_date);
  my $to_date_str = date_str(@to_date);
  my $group_id = sprintf("%02d%02d", $from_date[0], $from_date[1]);
  my $sch_id = "$job_id-$group_id";
  my $log_base_path = "$root_path/log/$sch_id";
  my $cmd_suffix = "> $log_base_path.log 2> $log_base_path.err";
  my $cmd = "$cmd_prefix $job_id scheduleId=$sch_id groupId=$group_id fromDate=$from_date_str toDate=$to_date_str @params $cmd_suffix";
  make_path("$root_path/log");
  print "$cmd\n\n";
  print "$log_base_path.log created.\n\n";
}

sub date_value {
  my ($date) = @_;
  return int($date / 100) * 12 + (($date - 1) % 100);
}

sub get_date {
  my ($date_value) = @_;
  return (int($date_value / 12), 1 + $date_value % 12);
}

sub date_str {
  return sprintf("%02d-%02d-01T00:00:00+0800", $_[0], $_[1]);
}

sub bound_value {
  my $min = $_[1] > $_[2] ? $_[2] : $_[1];
  my $max = $_[1] > $_[2] ? $_[1] : $_[2];
  return min($max, max($min, $_[0]));
}