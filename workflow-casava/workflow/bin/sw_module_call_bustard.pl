use strict;
use Data::Dumper;
use Getopt::Long;

###
#  Copyright (C) 2014  Ontario Institute of Cancer Research
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
# 
# Contact us:
# 
#  Ontario Institute for Cancer Research  
#  MaRS Centre, West Tower
#  661 University Avenue, Suite 510
#  Toronto, Ontario, Canada M5G 0A3
#  Phone: 416-977-7599
#  Toll-free: 1-866-678-6427
#  www.oicr.on.ca
###


##########
#
# Script:  sw_module_call_bustard.pl
# Purpose: calls the Illumina tools for converting BCL files to FASTQ for one lane of sequencing. Also supports creating BCLs from Intensities if do_olb is enabled.
# 
#	* Means an argument is required
# Input:	BASE CALLING
#		--bcl-to-fastq		: *the path to bcl2fastq.pl
#	   	--intensity-folder 	: *an intensity folder from a sequencing run
#		--called-bases-folder	: *the folder containing the BCL files in the sequencing run
#		--flowcell		: *the name of the flowcell (sequencer run)
#		--lane			: *the lane number to perform bcl2fastq on
#		--tile			: the sequencing tile. Primarily for testing.
#		--barcodes		: *A "+" separated list of barcodes, accessions, sample names which are separated by comma.
#					  Used in the Sample sheet and in the final file name 
#					  For example: AATC,121212,sample1+AATG,1238291,sample2
#		--output-dir		: *the location where the final files should be placed
#		--mismatches		: number of mismatches to allow in the barcode (default: 1)
#		--ignore-missing-bcl	: flag passed to bcl2fastq, allows missing bcl files
#		--ignore-missing-stats	: flag passed to bcl2fastq, allows missing stat files
#		--use-bases-mask	: specify the bases mask for bcl2fastq
#		--threads 		: *the number of threads to use when running bcl2fastq
#		--other-bcltofastq-options : any other options that will be passed directly to bcl2fastq
#		
#		BUSTARD/OLB
#		--do-olb		: flag to perform Bustard (create BCLs from Intensity files)
#		--cleanup		: flag to clean up Bustard results after run
#		--other-bustard-options	: any other options that will be passed directly to bustard
#		--bustard		: the path to the bustard.py script if --do-olb is enabled
#		--help			: flag to print usage
#
# Output:  	a directory with the results from bcl2fastq
#
##########

my ($intensity_folder, $flowcell, $lane, $barcodes, $bclToFastq, $bustard_threads, $bustard, $output_dir, $help, $cleanup, $tile, $called_bases_folder, $do_olb, $mismatches, $ignore_missing_bcl, $ignore_missing_stats, $mask, $other_bcltofastq_options, $other_bustard_options);
$help = 0;
$cleanup = 0;
$do_olb = 0;
my $argSize = scalar(@ARGV);
my $getOptResult = GetOptions('intensity-folder=s' => \$intensity_folder, 'threads=i' => \$bustard_threads, 'bustard=s' => \$bustard, 'bcl-to-fastq=s' => \$bclToFastq, 'flowcell=s' => \$flowcell, 'lane=i' => \$lane, 'tile=i' => \$tile, 'barcodes=s' => \$barcodes, 'output-dir=s' => \$output_dir, 'cleanup' => \$cleanup, 'do-olb=i' => \$do_olb, 'called-bases-folder=s' => \$called_bases_folder, 'help' => \$help, 'mismatches=s' => \$mismatches, 'ignore-missing-bcl' => \$ignore_missing_bcl, 'ignore-missing-stats' => \$ignore_missing_stats, 'use-bases-mask=s' => \$mask, 'other-bcltofastq-options=s' => \$other_bcltofastq_options, 'other-bustard-options=s' => \$other_bustard_options);
usage() if (!$getOptResult || $help);
usage() if (not defined $bclToFastq || not defined $intensity_folder || not defined $bustard_threads || not defined $flowcell || not defined $lane || not defined $barcodes ||  not defined $output_dir || not defined $called_bases_folder);
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
foreach my $barcode_record (@barcode_arr) {
  my @barcode_record_arr = split /,/, $barcode_record;
  my $barcode = $barcode_record_arr[0];
  my $ius_accession = $barcode_record_arr[1];
  my $ius_ass_sample_str = $barcode_record_arr[2];
  print OUT "$flowcell,$lane,SWID_$ius_accession\_$ius_ass_sample_str\_$flowcell,na,$barcode,na,N,na,na,na\n";
}
close OUT;

# now run bcl2fastq
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
  print "usage: sw_module_call_bustard.pl --intensity-folder IlluminaIntensityFolder --threads int --bcl-to-fastq path_to_configureBclToFastq.pl --flowcell flowcell_name --lane int --barcodes AATC,121212;AATG,1238291 --output-dir output_dir_path [[--cleanup]] [[--ignore-missing-bcl]] [[--ignore-missing-stats]] [[--use-bases-mask *mask*]] [[--mismatches *num_of_mismatches per barcode component*]] [[--do-olb]] [[--bustard path_to_bustard.py]] [[--other-bcltofastq-options \"other opts\"]] [[--other-bustard-options \"other opts\"]] [[--help|-?]\n";
  exit(1);
}

