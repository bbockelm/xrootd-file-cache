#!/usr/bin/env perl

use strict;
use warnings;

#my $path = "test.log";
my $path =  shift(@ARGV);

open( my $fh, '<', $path)
    or die "Could not open $path!";

# Detach: NumHits[5] NumHitsPrefetch[5] NumHitsDisk[0], NumMissed [1061],
# BytesGet[561920343697072] BytesGetPrefetch[140653338155696] BytesGetDisk[140648476470272], BytesPass[140652595763376], BytesWrite[140653588801700]  
#BytesGet[10082080] BytesGetPrefetch[10082080] BytesGetDisk[0], BytesPass[2137401568], BytesWrite[4201200557] root://18@cmsstor314.fnal.go


my $reHits = '/(.*)Detach: NumHits\[(\d+)\] NumHitsPrefetch\[(\d+)\] NumHitsDisk\[(\d+)\] NumMissed\[(\d+)\] (.*)';

my $reBytes = 'BytesGet\[(\d+)\] BytesGetPrefetch\[(\d+)\] BytesGetDisk\[(\d+)\] BytesPass\[(\d+)\] BytesWrite\[(-?\d*)\]';

#my $re = '/(.*)IO: Detach  NumHits\[(.*)\] NumHitsPrefetch\[(.*)\] NumHitsDisk\[(.*)\], NumMissed \[(.*)\],BytesGet\[(.*)\] BytesGetPrefetch\[(.*)\] BytesGetDisk\[(.*)\]';
# my $re = '/(.*)IO: Detach  NumHits\[(.*)\] NumHitsPrefetch\[(.*)\] NumHitsDisk\[(.*)\], NumMissed \[(.*)\],BytesGet\[(.*)\] BytesGetPrefetch\[(.*)\]';
#my $re = '/(.*)IO: Detach  NumHits\[(.*)\] NumHitsPrefetch\[(.*)\] NumHitsDisk\[(.*)\], NumMissed \[(.*)\],BytesGet\[(.*)\]';
#IO: Detach  NumHits[0] NumHitsPrefetch[0] NumHitsDisk[0], NumMissed [0],BytesGet[0] BytesGetPrefetch[0] BytesGetDisk[0], BytesPass[0], BytesWrite[-1] root://93@cmss   


#my $re = '/(.*)IO: Detach  NumHits\[(.*)\] NumHitsPrefetch\[(.*)\] NumHitsDisk\[(.*)\], NumMissed \[(.*)\], BytesGet\[(.*)\] BytesGetPrefetch\[(.*)\] BytesGetDisk\[(.*)\], BytesPass\[(.*)\], BytesWrite\[(.*)\] (.*)';
my $c = 0;

my $sumGet = 0;
my $sumGetPrefetch = 0;
my $sumGetDisk = 0;

my$sumPass = 1;
my$sumWrite = 0;

while (my $line = <$fh>){ 
  # print $line;
  chomp $line;

  if($line =~ /$reHits/ ) 
{
  # file name
  my $path       = $1;


  # hits count;
  my $hits       = $2;
  my $hitsPrefetch       = $3;
  my $hitsDisk       = $4;
  my $missed     = $5;
 # print $line, "\n\n";
 # print "=== ", $2,", ", $3,", ", $4,", ", $5, "==== NHITS ===================\n";

  # bytes
  my $bytes = $6;
#  print "gggg BYE ", $bytes, "\n";
  if($bytes =~ /$reBytes/ ) { 
  #  print $1, ", ", $2, ", ", $3, ", ", $4, ", ",  $5, "========= BYTES ==========\n";

    my $bytesGet   = $1;
    my $bytesGetPrefetch   = $2;
    my $bytesGetDisk   = $3;
    my $bytesPass  = $4;
    my $bytesWrite = $5;

    my $name;
    if ($path =~ /(.*)\/(.*)\?/) {
      $name = $2;
    }
    printf( "hit =  %-5s miss = %-5s | byteGet %-12s = [ %-10s + %-10s],  bytesPass %-12s, bytesWrite %-12s %-12s\n", $hits, $missed, $bytesGet, $bytesGetPrefetch, $bytesGetDisk, $bytesPass, $bytesWrite, $name); 

    $sumGet += $bytesGet;
    $sumGetPrefetch += $bytesGetPrefetch;
    $sumGetDisk += $bytesGetDisk;

    $sumPass += $bytesPass;
    $sumWrite += $bytesWrite;


  }




  if ($hits == 0 && $missed == 0) { 
#print " \n !!! detach \n";
    next;
  }
  
}
}

printf( "------------\n summaGet/semPass = %d/%d =  %.2f, sumWrite %d \n", $sumGet, $sumPass, $sumGet/$sumPass, $sumWrite); 
printf("----- pred/disc = %d/%d \n", $sumGetPrefetch, $sumGetDisk);





