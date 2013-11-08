use strict;
use Data::Dumper;
use Getopt::Long;

##########
#
# Script:  sw_module_call_bustard.pl
# Date:    20110901
# Author:  Brian O'Connor <briandoconnor@gmail.com>
#
# Purpose: this just calls the bustard tools to do base calling for a particular lane
#          It assumes something like /oicr/local/analysis/illumina/OLB-1.9.3/bin/bustard.py /.mounts/sata/bas013/archive/h239/110525_h239_0118_AC00ULACXX/Data/Intensities --CIF --make has already been called on this intensity dir
#          
# Input:   an intensity folder. The barcode param is special, it's a semicolon seperated list of barcode and parent accessions which are seperated by comma.  For example: AATC,121212+AATG,1238291,sample_name
#
# Output:  a bustard directory
#
# FIXME: core problem with this is I don't know how many IUS fastqs I will get and I don't know there file name.
# The solution is to reimplement this script as a Java module so I can figure out the output files and correctly save them to the DB.
# The decider will probably have to save the IUS sw_accession for each barcode so the output file can be linked back correctly
#
# FIXME: how do we deal with cleanup? This is going to leave a lot of junk in the run folder.
#
# FIXME: to prevent a huge acount of file redundancy this command will need to run on a per lane basis, should split the barcodes and ius by ","
# 
# FIXME: need to remove tile limit for production
#
##########

my ($intensity_folder, $flowcell, $lane, $barcodes, $bclToFastq, $bustard_threads, $bustard, $output_dir, $help, $cleanup, $tile, $called_bases_folder, $do_olb, $mismatches, $ignore_missing_bcl, $ignore_missing_stats, $mask, $other_bcltofastq_options, $other_bustard_options);
$help = 0;
$cleanup = 0;
$do_olb = 0;
my $argSize = scalar(@ARGV);
my $getOptResult = GetOptions('intensity-folder=s' => \$intensity_folder, 'threads=i' => \$bustard_threads, 'bustard=s' => \$bustard, 'bcl-to-fastq=s' => \$bclToFastq, 'flowcell=s' => \$flowcell, 'lane=i' => \$lane, 'tile=i' => \$tile, 'barcodes=s' => \$barcodes, 'output-dir=s' => \$output_dir, 'cleanup' => \$cleanup, 'do-olb=i' => \$do_olb, 'called-bases-folder=s' => \$called_bases_folder, 'help' => \$help, 'mismatches=s' => \$mismatches, 'ignore-missing-bcl' => \$ignore_missing_bcl, 'ignore-missing-stats' => \$ignore_missing_stats, 'use-bases-mask=s' => \$mask, 'other-bcltofastq-options=s' => \$other_bcltofastq_options, 'other-bustard-options=s' => \$other_bustard_options);
usage() if ( $argSize < 18 || !$getOptResult || $help);

###########################################################################################################################

# original dir
my $orig_dir = `pwd`;
chomp $orig_dir;
print "Original dir: $orig_dir\n";

my $bustard_dir = "";

if($do_olb) {

  # setup bustard
  if (!-d $intensity_folder) { print "Error: can't find intensity folder: $intensity_folder\n"; exit(1); }
  # for testing
  #my $cmd = "$bustard $intensity_folder --CIF --tiles=s_${lane}_1305 --make";
  my $cmd = "$bustard $intensity_folder --CIF --tiles=s_${lane} --make";
  if (defined($tile) && $tile ne "") {
    $cmd = "$bustard $intensity_folder --CIF --tiles=s_${lane}_${tile} --make";
  }
  if (defined $other_bustard_options) {
    $cmd .= " $other_bustard_options"
  }
  
  print "Running: $cmd\n";
  my $output = `$cmd`;
  if ($? != 0) { print "Error: bustard command returned a non-zero exit code $?\n"; exit(1); }
  $output =~ /Sequence folder: (\S+)/;
  $bustard_dir = $1;
  print "Bustard dir: $bustard_dir\n";
  if (!-d $bustard_dir) { print "Errors! bustard dir does not exist: $bustard_dir\n"; exit(1); }
  
  # run bustard to call bases
  chdir($bustard_dir);
  print "Running: make -j ${bustard_threads}\n";
  my $result = system("make -j ${bustard_threads}");
  if ($result != 0) { print "Errors! exit code: $result\n"; exit(1); }
  
  # try for a second time since I've seen the first time fail without error return
  $result = system("make -j ${bustard_threads}");
  if ($result != 0) { print "Errors! exit code: $result\n"; exit(1); }
  
  # should see this file, if not this has failed
  if (!-e "finished.txt") {
  	print "Error! can't find the file $bustard_dir/finished.txt\n"; exit(1);
  }

} else {
  # if the base directory was passed then swap it out for the bustard
  $bustard_dir = $called_bases_folder;
}

# convert BCL to Fastq for this lane

# Example samplesheet:
#FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject
#C00ULACXX,1,ACC_0001_Ly_R_PE_350_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,2,ACC_0001_Ly_R_PE_350_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,3,ACC_0001_Ly_R_PE_382_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,4,ACC_0001_Ly_R_PE_382_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,5,ACC_0002_Ly_R_PE_329_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,6,ACC_0002_Ly_R_PE_329_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,7,ACC_0002_Ly_R_PE_378_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC
#C00ULACXX,8,ACC_0002_Ly_R_PE_378_WG,hg19_random,,"Reference Tissue, 378bp fragment length",N,unknown,ltimms,ACC

# go back to original dir
chdir($orig_dir);

# create a temp sample file that just includes this lane
my $result = system("mkdir -p $output_dir/Unaligned_${flowcell}_${lane}");
if ($result != 0) { print "Errors! exit code: $result\n"; exit(1); }

open OUT, ">$output_dir/Unaligned_${flowcell}_${lane}/metadata_${flowcell}_${lane}.csv" or die "Can't open file $output_dir/Unaligned_${flowcell}_${lane}/metadata_${flowcell}_${lane}.csv";
print OUT "FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject\n";
my @barcode_arr = split /\+/, $barcodes;
#add by zheng, temporary fix
#add check for one barcode perl lane, this is a lims bug.
if ( scalar @barcode_arr == 1 ) {
    my $barcode_record = $barcode_arr[0];
    my @barcode_record_arr = split /,/, $barcode_record;
    #barcode will NOT be used
    my $barcode = $barcode_record_arr[0];
    my $ius_accession = $barcode_record_arr[1];
    my $ius_ass_sample_str = $barcode_record_arr[2];
    print OUT "$flowcell,$lane,SWID_$ius_accession\_$ius_ass_sample_str\_$flowcell,na,,na,N,na,na,na\n";
}
else {
foreach my $barcode_record (@barcode_arr) {
  my @barcode_record_arr = split /,/, $barcode_record;
  my $barcode = $barcode_record_arr[0];
  my $ius_accession = $barcode_record_arr[1];
  my $ius_ass_sample_str = $barcode_record_arr[2];
  print OUT "$flowcell,$lane,SWID_$ius_accession\_$ius_ass_sample_str\_$flowcell,na,$barcode,na,N,na,na,na\n";
}
}
close OUT;

# now run the next step
# FIXME: should param this fastq cluster count, using 1000x the max recommended by docs, I just want one file per lane!! (16000000)
# BUG: this is going to cause problems when the density per lane exceeds this, we won't records more than one fastq/fastq pair per IUS!
if (! defined $mismatches) {$mismatches = '1';}
my $cmd = "$bclToFastq --force --fastq-cluster-count 1600000000 --input-dir $bustard_dir --output-dir $output_dir/Unaligned_${flowcell}_${lane} --intensities-dir $intensity_folder --sample-sheet $output_dir/Unaligned_${flowcell}_${lane}/metadata_${flowcell}_${lane}.csv --mismatches $mismatches";
if (defined($tile) && $tile ne "") {
    $cmd .= " --tiles s_${lane}_${tile}";
}
if (defined $ignore_missing_bcl) {
  $cmd .= ' --ignore-missing-bcl';
}
if (defined $ignore_missing_stats) {
  $cmd .= ' --ignore-missing-stats';
}
if (defined $mask) {
  $cmd .= " --use-bases-mask $mask"
}
if (defined $other_bcltofastq_options) {
  $cmd .= " $other_bcltofastq_options"
}
print "Running: $cmd\n";
$result = system($cmd);
if ($result != 0) { print "Errors! exit code: $result\n"; exit(1); }

# run the actual demultiplex and conversion from bcl to fastq
chdir("$output_dir/Unaligned_${flowcell}_${lane}");
print "Running: make -j ${bustard_threads}\n";
$result = system("make -j ${bustard_threads}");
if ($result != 0) { print "Errors! exit code: $result\n"; exit(1); }

# cleanup the bustard dir
if ($do_olb && $bustard_dir =~ /Bustard/ && -e $bustard_dir && $cleanup) {
  print "Cleaning up the Bustard directory: $bustard_dir\n";
  $result = system("rm -rf $bustard_dir");
  if ($result != 0) { print "Errors! exit code: $result\n"; exit(1); }
}

exit(0);

###########################################################################################################################

sub usage {
  print "Unknown option: @_\n" if ( @_ );
  print "usage: program [--intensity-folder IlluminaIntensityFolder] [--threads int] [--bustard path_to_bustard.py] [--bcl-to-fastq path_to_configureBclToFastq.pl] [--flowcell flowcell_name] [--lane int] [--barcodes AATC,121212;AATG,1238291] [--output-dir output_dir_path] [[--cleanup]] [[--ignore-missing-bcl]] [[--ignore-missing-stats]] [[--use-bases-mask *mask*]] [[--mismatches *num_of_mismatches per barcode component*]] [[--help|-?]\n";
  exit(1);
}

