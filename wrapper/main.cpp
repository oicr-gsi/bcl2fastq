#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fnmatch.h>
#include <fstream>
#include <gzstream.h>
#include <initializer_list>
#include <iostream>
#include <json/json.h>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdio.h>
#include <sys/ptrace.h>
#include <sys/reg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

// This program tries to run bcl2fastq without writing out an Undetermined file
// in a way that's friendly to WDL. The Undetermined file contains the reads
// that belong to no sample, but we never use it for anything and it can be
// massive (e.g., extracting a single sample from a NovaSeq run, it can be
// terabytes). There's no option to turn it off and trying to guile bcl2fastq
// doesn't work. It does that in the following major steps:
//
// 1. (main) parses the command line arguments to get the sample JSON
// 2. (main) prepares commandline arguments for bcl2fastq (what was provided on
//           the command line + some additional ones)
// 3. (main+run_process) run the program using ptrace to intercept calls to
//                       open() to send the undetermined to /dev/null
// 4. (main+Sample::process) find the output FASTQs and read the Stats.json
//                           to prepare a list of files and read counts for
//                           the WDL
// 4. (Sample::process+concat_files) spin up a thread to concatenate FASTQs
// files
// 5. (main) wait for threads to finish
// 6. (main) write out file list and read counts as JSON

// Concatenate input files
static void concat_files(std::string outputFile,
                         std::vector<std::string> inputFiles) {
  std::cerr << "Concatenation for " << inputFiles.size() << " input files into "
            << outputFile << " is running." << std::endl;

  std::ofstream output(outputFile);
  for (auto &inputFile : inputFiles) {
    std::cerr << "Draining " << inputFile << " into " << outputFile << "..."
              << std::endl;
    std::ifstream input(inputFile);
    if (input.good()) {
      output << input.rdbuf();
    } else {
      std::cerr << "Failed to open " << inputFile << "!" << std::endl;
      abort();
    }
  }
  std::cerr << "Concatenated " << inputFiles.size() << " input files into "
            << outputFile << "." << std::endl;
}

class Sample {
public:
  virtual ~Sample() = default;
  virtual void process(std::vector<std::thread> &threads, std::set<int> &reads,
                       const std::string &outputDir, Json::Value &stats_json,
                       Json::Value &output,
                       std::vector<std::string> &fastqs) = 0;
};

Json::LargestInt findReadCount(const std::string &targetName,
                               const Json::Value &stats_json) {
  for (Json::ArrayIndex i = 0; i < stats_json["ConversionResults"].size();
       i++) {
    auto &cr = stats_json["ConversionResults"][i];
    for (Json::ArrayIndex j = 0; j < cr["DemuxResults"].size(); j++) {
      auto &dr = cr["DemuxResults"][j];
      if (dr["SampleName"].asString() == targetName) {
        return dr["NumberReads"].asLargestInt();
      }
    }
  }
  return 0;
}

// Store information about a sample in the input (might map to multiple rows in
// the sample sheet if multiple barcodes)
class SimpleSample : public Sample {
public:
  SimpleSample(Json::ArrayIndex id_, const std::string &name_, int barcodes_)
      : id(id_), barcodes(barcodes_), name(name_) {}

  // Find any matching files and spin up a concatenation thread for each one;
  // find the read count and write out JSON data with the output files and read
  // count
  void process(std::vector<std::thread> &threads, std::set<int> &reads,
               const std::string &outputDir, Json::Value &stats_json,
               Json::Value &output, std::vector<std::string> &fastqs) {

    std::cerr << "Starting for " << name << "..." << std::endl;

    Json::LargestInt readCount = 0;
    // For each of the input barcodes, pick up the matching read count
    for (auto barcode = 0; barcode < barcodes; barcode++) {
      std::stringstream targetName;
      auto found = false;
      targetName << "sample" << id << "_" << barcode;
      readCount += findReadCount(targetName.str(), stats_json);
    }

    std::cerr << "Getting read count " << name << " from Stats/Stats.json."
              << std::endl;

    for (auto &read : reads) {
      std::stringstream filename;
      filename << name << "_R" << read << ".fastq.gz";

      // Make the output structure of the form {"left":"name_R1.fastq.gz",
      // "right":{"read_count":9000, "read_number":1}}
      std::cerr << "Got " << readCount << " reads for " << name << "."
                << std::endl;
      Json::Value record(Json::objectValue);
      Json::Value pair(Json::objectValue);
      Json::Value attributes(Json::objectValue);
      attributes["read_count"] = readCount;
      attributes["read_number"] = read;
      pair["left"] = filename.str();
      pair["right"] = std::move(attributes);
      record["fastqs"] = std::move(pair);
      record["name"] = name;
      output.append(std::move(record));

      std::vector<std::string> inputFiles;
      // Rummage through the directory for what looks like all the fastqs
      for (auto barcode = 0; barcode < barcodes; barcode++) {
        std::stringstream samplesheetpattern;
        samplesheetpattern << "sample" << id << "_" << barcode << "_S*_R"
                           << read << "_001.fastq.gz";

        for (auto &fastq : fastqs) {
          // Match the found strings using the glob-like matching provided by
          // fnmatch; do not allow * to cross directory boundaries
          // (FNM_PATHNAME)
          switch (fnmatch(samplesheetpattern.str().c_str(), fastq.c_str(),
                          FNM_PATHNAME)) {
          case 0:
            inputFiles.push_back(outputDir + "/" + fastq);
            break;
          case FNM_NOMATCH:
            break;
          default:
            std::cerr << "Failed to perform fnmatch on " << fastq << " for "
                      << samplesheetpattern.str() << "." << std::endl;
          }
        }
      }
      if (inputFiles.empty()) {
        // Write empty file
        ogzstream(filename.str().c_str());
        std::cerr << "No data for " << filename.str()
                  << ". Creating empty file." << std::endl;
      } else {
        // Create a new thread for doing the concatenation.
        std::cerr << "Starting thread for " << filename.str()
                  << " concatenation from " << inputFiles.size() << " files."
                  << std::endl;
        std::thread copier(concat_files, filename.str(), inputFiles);
        threads.push_back(std::move(copier));
      }
    }
  }

private:
  Json::ArrayIndex id;
  int barcodes;
  std::string name;
};

static void
extract_umi(std::vector<std::string> commandLine,
            std::vector<std::reference_wrapper<Json::Value>> readCountOutput,
            std::string metrics) {
  const auto pid = fork();
  if (pid < 0) {
    // Fork failed. Not much we can do
    perror("fork");
  } else if (pid == 0) {
    // This is the child process; run barcodex
    errno = 0;
    char *child_args[commandLine.size() + 1];
    std::cerr << "Going to excute:";
    for (auto i = 0; i < commandLine.size(); i++) {
      child_args[i] = strdup(commandLine[i].c_str());
      std::cerr << " " << commandLine[i];
    }
    std::cerr << std::endl;
    child_args[commandLine.size()] = nullptr;
    execvp(child_args[0], child_args);
    perror("execvp");
    exit(1);
  } else {
    // Wait for the child to exit.
    while (true) {
      int status = 0;
      errno = 0;
      if (waitpid(pid, &status, 0) < 0) {
        if (errno == EINTR) {
          continue;
        }
        perror("waitpid");
        return;
      }
      if (WIFEXITED(status)) {
        std::cerr << "barcodex exited with " << WEXITSTATUS(status)
                  << std::endl;
        if (WEXITSTATUS(status) == 0) {
          // Read the extraction metrics output and update the read counts
          Json::Value stats_json;
          std::ifstream stats_data(metrics);
          stats_data >> stats_json;
          for (Json::Value &value : readCountOutput) {
            value["read_count"] =
                stats_json["reads/pairs with matching pattern"];
          }
        }
        return;
      } else if (WIFSIGNALED(status)) {
        std::cerr << "barcodex signalled " << strsignal(WTERMSIG(status))
                  << std::endl;
        return;
      }
    }
  }
}

// Store information about a sample in the input (might map to multiple rows in
// the sample sheet if multiple barcodes) and needs UMI extraction done on the
// output
class UmiSample : public Sample {
public:
  UmiSample(Json::ArrayIndex id_, const std::string &name_, int barcodes_,
            bool is_inline_, const std::string &acceptableUmiList_,
            const Json::Value &patterns_)
      : id(id_), acceptableUmiList(acceptableUmiList_), barcodes(barcodes_),
        is_inline(is_inline_), name(name_) {
    // Each read is associated with a UMI pattern (regex) that is handed to us
    // as a JSON object with the key as a string contianing a number and the
    // regex as the value. This extracts those patterns into a map so we can
    // generate command line argument for barcodex. Techincally, this entire
    // dictionary can be null, so there's a guard. barcodex is likely to fail,
    // but we're going to run it anyway.
    if (patterns_.isObject()) {
      auto memberNames = patterns_.getMemberNames();
      for (auto i = 0; i < memberNames.size(); i++) {
        patterns[atoi(memberNames[i].c_str())] =
            patterns_[memberNames[i]].asString();
      }
    }
  }

  void process(std::vector<std::thread> &threads, std::set<int> &reads,
               const std::string &outputDir, Json::Value &stats_json,
               Json::Value &output, std::vector<std::string> &fastqs) {

    // There are two modes for UMIs: inline where the barcode is included in the
    // existing reads and where the barcode is the last read, so take the number
    // of reads we've actually extracted and compute the number that we are
    // going to output.
    auto usefulMaxRead =
        *std::max_element(reads.begin(), reads.end()) - (is_inline ? 0 : 1);

    std::cerr << "Starting for " << name << " which will output "
              << usefulMaxRead << " useful reads after extraction..."
              << std::endl;

    // We're going to get barcodex to do all the heavy lifting, so let's build
    // up its command line
    std::vector<std::string> commandLine{"barcodex-rs",
                                         "--separator",
                                         ":",
                                         "--prefix",
                                         name,
                                         "--umilist",
                                         acceptableUmiList,
                                         is_inline ? "inline" : "separate"};
    std::vector<std::string> outputFastqs;
    // These patterns are provided by the input and tell barcodex what to
    // extract from the reads
    if (is_inline) {
      for (auto &entry : patterns) {
        if (entry.first <= usefulMaxRead) {
          std::stringstream parameter;
          parameter << "--pattern" << entry.first;
          commandLine.push_back(parameter.str());
          commandLine.push_back(entry.second);
        }
      }
    }

    // barcodex is going possibly throw away some reads, so the read count
    // proided to us from bcl2fastq isn't going to be useful. We're going to
    // create all the data structures Cromwell needs for the output files now,
    // and store a list of ones that need to be updated with the barcodex read
    // count once we're done.
    std::vector<std::reference_wrapper<Json::Value>> readCountOutput;

    // We need to provision out the metadata that's global for the sample
    for (auto &suffix : {"_UMI_counts.json", "_extraction_metrics.json"}) {
      std::stringstream filename;
      filename << name << suffix;
      Json::Value record(Json::objectValue);
      Json::Value pair(Json::objectValue);
      Json::Value attributes(Json::objectValue);
      attributes["read_count"] = 0;
      pair["left"] = filename.str();
      pair["right"] = std::move(attributes);
      record["fastqs"] = std::move(pair);
      record["name"] = name;
      output.append(std::move(record));
      // We're going to need to update these later, so hold references to them
      // that we can pass to the other thread.
      readCountOutput.push_back((*--output.end())["fastqs"]["right"]);
    }

    // Now we need to handle every read
    auto hasInput = false;
    for (auto &read : reads) {

      for (auto suffix : {"extracted", "discarded"}) {
        std::stringstream filename;
        filename << name << "_R" << read << "." << suffix << ".fastq.gz";

        Json::Value record(Json::objectValue);
        Json::Value pair(Json::objectValue);
        Json::Value attributes(Json::objectValue);
        attributes["read_count"] = 0;
        attributes["read_number"] = -read;
        attributes["umi_extraction"] = suffix;
        pair["left"] = filename.str();
        pair["right"] = std::move(attributes);
        record["fastqs"] = std::move(pair);
        record["name"] = name;
        output.append(std::move(record));
        // We're going to need to update these later, so hold references to
        // them that we can pass to the other thread.
        readCountOutput.push_back((*--output.end())["fastqs"]["right"]);
        outputFastqs.push_back(filename.str());
      }
      // In non-inline kits, the final read will be consumed and not provisioned
      // out
      if (read <= usefulMaxRead) {
        // We're going to provision out 3 files: a base file, an extracted file,
        // and a discarded file. We're going to set the read_number annotation
        // on the extracted and discarded files to be negative so downstream
        // proceses that are looking for reads don't choke.
        std::stringstream output_filename;
        output_filename << name << "_R" << read << ".fastq.gz";
        outputFastqs.push_back(output_filename.str());

        // Prepare the output information for the main generated FASTQ
        Json::Value record(Json::objectValue);
        Json::Value pair(Json::objectValue);
        Json::Value attributes(Json::objectValue);
        attributes["read_count"] = 0;
        attributes["read_number"] = read;
        attributes["umi_extraction"] = "output";
        pair["left"] = output_filename.str();
        pair["right"] = std::move(attributes);
        record["fastqs"] = std::move(pair);
        record["name"] = name;
        output.append(std::move(record));
        // We're going to need to update these later, so hold references to
        // them that we can pass to the other thread.
        readCountOutput.push_back((*--output.end())["fastqs"]["right"]);
      }

      for (auto barcode = 0; barcode < barcodes; barcode++) {
        // Rummage through the directory for what looks like all the fastqs
        // Even if we aren't provisiong out a matching extract, we still need to
        // tell barcodex about them
        std::stringstream samplesheetpattern;
        samplesheetpattern << "sample" << id << "_" << barcode << "_S*_R"
                           << read << "_001.fastq.gz";
        std::stringstream parameter;
        parameter << "--r";
        if (is_inline || read <= usefulMaxRead) {
          parameter << read;
        } else {
          parameter << "u";
        }
        parameter << "-in";

        for (auto &fastq : fastqs) {
          // Match the found strings using the glob-like matching provided by
          // fnmatch; do not allow * to cross directory boundaries
          // (FNM_PATHNAME)
          switch (fnmatch(samplesheetpattern.str().c_str(), fastq.c_str(),
                          FNM_PATHNAME)) {
          case 0:
            commandLine.push_back(parameter.str());
            commandLine.push_back(outputDir + "/" + fastq);
            hasInput = true;
            break;
          case FNM_NOMATCH:
            break;
          default:
            std::cerr << "Failed to perform fnmatch on " << fastq << " for "
                      << samplesheetpattern.str() << "." << std::endl;
          }
        }
      }
    }
    if (hasInput) {
      // Create a new thread for doing the extraction.
      std::cerr << "Starting thread for " << name << " UMI extraction."
                << std::endl;
      std::thread copier(extract_umi, commandLine, readCountOutput,
                         name + "_extraction_metrics.json");
      threads.push_back(std::move(copier));
    } else {
      // Whelp, nothing to extract. Set everything to have zero reads
      for (Json::Value &value : readCountOutput) {
        value["read_count"] = 0;
      }
      // Write empty FASTQs
      for (auto &fastq : outputFastqs) {
        ogzstream(fastq.c_str());
      }

      // For the UMI counts, we write a JSON file with an empty object in it
      std::stringstream counts_filename;
      counts_filename << name << "_UMI_counts.json";
      Json::Value output_counts_obj(Json::objectValue);
      std::ofstream output_counts_data(counts_filename.str());
      output_counts_data << output_counts_obj;

      // Then we write out some sad stats
      std::stringstream metrics_filename;
      metrics_filename << name << "_extraction_metrics.json";
      Json::Value output_metrics_obj(Json::objectValue);
      output_metrics_obj["total reads/pairs"] = 0;
      output_metrics_obj["reads/pairs with matching pattern"] = 0;
      output_metrics_obj["discarded reads/pairs"] = 0;
      output_metrics_obj["discarded reads/pairs due to unknown UMI"] = 0;
      output_metrics_obj["umi-list file"] = acceptableUmiList;

      for (auto &entry : patterns) {
        std::stringstream key;
        key << "pattern" << entry.first;
        output_metrics_obj[key.str()] = entry.second;
      }

      std::ofstream output_metrics_data(metrics_filename.str());
      output_metrics_data << output_metrics_obj;
    }
  }

private:
  Json::ArrayIndex id;
  std::string acceptableUmiList;
  int barcodes;
  bool is_inline;
  std::string name;
  std::map<int, std::string> patterns;
};

// Possibly overwrite a file name before doing a system call
// child: the child process monitored
// filename_register: the register that will hold the file name during the
// system call
// search_filename: a bit of text to signal the file needs to be rewritten
// replacement_filename: the replacement text to overwrite the file name with.
// This must be zero-padded to be a multiple of sizeof(long) because we can only
// access the child's memory in long-sized chunks
static void conditional_rewrite(pid_t child, int filename_register,
                                const char *search_filename,
                                const char *replacement_filename,
                                size_t replacement_filename_len) {
  // Look into the child process's register for the address of the string it's
  // sending as the file name to the system call and store that address
  errno = 0;
  auto filename_addr =
      ptrace(PTRACE_PEEKUSER, child, sizeof(long) * filename_register, 0);
  auto current_filename_addr = filename_addr;
  const char *current_search_filename = search_filename;
  while (true) {
    union {
      long word;
      char c[sizeof(long)];
    } value;
    // Read a long's worth of the file name child's into our memory space;
    // long-sized is part of the ptrace API
    errno = 0;
    value.word = ptrace(PTRACE_PEEKDATA, child, current_filename_addr, NULL);
    if (value.word == -1) {
      perror("ptrace PEEK");
      exit(1);
    }
    current_filename_addr += sizeof(long);

    // Now, look at that memory, as character, and keep track of how much of
    // the search string we've seen
    for (auto i = 0; i < sizeof(long); i++) {
      if (value.c[i] == '\0') {
        return;
      }
      if (value.c[i] == *current_search_filename) {
        // We've seen the whole thing. Kill it.
        if (*(++current_search_filename) == '\0') {
          std::cerr << "Intercepting access to " << search_filename
                    << std::endl;
          // We are going to put replacement_filename in, which is shorter than
          // the original. It has to be shorter because we are overwriting the
          // existing string, so we need it to fit in that memory.  We're
          // rounding everything in long-sized chunks, but
          // meh.
          for (auto i = 0; i < replacement_filename_len / sizeof(long); i++) {
            if (ptrace(PTRACE_POKEDATA, child, filename_addr,
                       ((long *)replacement_filename)[i]) < 0) {
              perror("ptrace POKE");
              return;
            }
            filename_addr += sizeof(long);
          }
          return;
        }
      } else {
        // Nope, reset what we're looking for
        current_search_filename = search_filename;
      }
    }
  }
}

// Padded with nulls so we can read it in long-sized blocks
const char devnull[] = {'/', 'd', 'e', 'v', '/', 'n', 'u', 'l',
                        'l', 0,   0,   0,   0,   0,   0,   0};
const char binls[] = {'/', 'b', 'i', 'n', '/', 'l', 's', 0};
// Based on:
// https://www.alfonsobeato.net/c/modifying-system-call-arguments-with-ptrace/
static bool run_process(pid_t child) {
  int data = 0;
  while (true) {
    int status = 0;
    errno = 0;
    if (ptrace(PTRACE_SYSCALL, child, 0, data) < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("ptrace: SYSCALL");
      return false;
    }
    errno = 0;
    if (waitpid(child, &status, 0) < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("waitpid");
      return false;
    }
    data = 0;
    /* Is it a syscall we care about? */
    if (WIFSTOPPED(status) && WSTOPSIG(status) & 0x80) {
      switch (ptrace(PTRACE_PEEKUSER, child, sizeof(long) * ORIG_RAX, 0)) {
      // When it tries to open a file, intervene.
      case 2: // open()
        static_assert(
            sizeof(devnull) % sizeof(long) == 0,
            "undetermined replacement path is not a multiple of long");
        conditional_rewrite(child, RDI, "Undetermined", devnull,
                            sizeof(devnull));
        break;
      case 257: // openat()
        conditional_rewrite(child, RSI, "Undetermined", devnull,
                            sizeof(devnull));
        break;
      // After extraction, it tries to stat its own output. That string has
      // been rewritten to /dev/null, whch is not a regular file and stat
      // returns results that make bcl2fastq unhappy. So, we're going to do a
      // second rewrite to /bin/ls, which is shorter than /dev/null and a
      // regular file.  This means there will probably be some garbage in the
      // log, but meh.
      case 4: // stat()
      case 6: // lstat()
        static_assert(sizeof(binls) % sizeof(long) == 0,
                      "stat replacement path is not a multiple of long");
        conditional_rewrite(child, RDI, "/dev/null", binls, sizeof(binls));
        break;
      }
    } else if (WIFEXITED(status)) {
      // The program exited, reap it and move along
      std::cerr << "bcl2fastq exited with " << WEXITSTATUS(status) << std::endl;
      return WEXITSTATUS(status) == 0;
    } else if (WIFSIGNALED(status)) {
      // The program signalled something unrelated to ptrace
      std::cerr << "bcl2fastq signalled " << strsignal(WTERMSIG(status))
                << std::endl;
      return false;
    } else if (WIFSTOPPED(status)) {
      // The child got a signal it needs to handle
      errno = 0;
      std::cerr << "Child needs to handle some " << strsignal(WSTOPSIG(status))
                << " signal." << std::endl;
      // Okay, so ptrace lets to decide whether or not we want to deliver this
      // signal, and we do, because I have no desire to deal with it, so tell
      // our ptrace call we want it to signal, unless it's the trap signal we
      // used to kick off the tracing.
      data = WSTOPSIG(status) == SIGTRAP ? 0 : WSTOPSIG(status);
    } else {
      std::cerr << "Unknown wait status for bcl2fastq." << std::endl;
      return false;
    }
  }
}

int main(int argc, char **argv) {

  // Process command line arguments. getopt will leave our arguments in
  // argv[opind..argc]
  char *sample_file = nullptr;
  char *temporary_directory = nullptr;
  bool help = false;
  int c;
  while ((c = getopt(argc, argv, "hs:t:")) != -1) {
    switch (c) {
    case 'h':
      help = true;
      break;
    case 's':
      sample_file = optarg;
      break;
    case 't':
      temporary_directory = optarg;
      break;
    }
  }

  if (help || !sample_file || !temporary_directory || argc - optind < 1) {
    fprintf(stderr, "Usage: %s -s samples.json -t /tmp/bcl2fastq/output -- "
                    "bcl2fastq arg1 arg2 \n",
            argv[0]);
    return 1;
  }

  // Convert the sample information into a sample sheet and a record for
  // post-processing the output
  auto samplesheet = std::string(temporary_directory) + "/samplesheet.csv";
  std::vector<std::unique_ptr<Sample>> sampleinfo;
  std::cerr << "Building sample sheet..." << std::endl;
  {
    // This is in a block to close all these files at the point we call
    // bcl2fastq
    Json::Value samples_json;
    std::ifstream sample_data(sample_file);
    sample_data >> samples_json;

    std::ofstream samplesheet_data(samplesheet);

    auto dual_barcoded = false;

    // The input format we expect from the WDL workflow is
    // [{"name":"outputFilePrefix", "barcodes":["AAAA"]}]; if multiple barcodes
    // are provided, they will be concatenated into one file.
    for (Json::ArrayIndex i = 0;
         !dual_barcoded && i < samples_json["samples"].size(); i++) {
      auto barcodes = samples_json["samples"][i]["barcodes"];
      for (Json::ArrayIndex b = 0; b < barcodes.size(); b++) {
        if (barcodes[b].asString().find("-") != std::string::npos) {
          dual_barcoded = true;
          break;
        }
      }
    }
    // For details on the sample sheet we are generating, see:
    // https://web.archive.org/web/20181225230522/https://www.illumina.com/content/dam/illumina-marketing/documents/products/technotes/sequencing-sheet-format-specifications-technical-note-970-2017-004.pdf
    // Since we are doing --no-lane-splitting, we don't include a lane column.
    // The columns can vary if there is a 2nd (i5) index. Most of the columns
    // are garbage that is only used for the Illumina QC software (called SAV)
    // or analysis we don't ask the machine to do, so we can fill it with
    // garbage.
    samplesheet_data << "[Header]\n\n[Data]\nSample_ID,Sample_"
                        "Name,Manifest,GenomeFolder,I7_Index_ID,Index";
    if (dual_barcoded) {
      samplesheet_data << ",I5_Index_ID,Index2";
    }
    samplesheet_data << "\n";

    for (Json::ArrayIndex i = 0; i < samples_json["samples"].size(); i++) {
      auto info = samples_json["samples"][i];
      auto &barcodes = info["barcodes"];
      if (info.isMember("acceptableUmiList") &&
          !info["acceptableUmiList"].isNull()) {
        sampleinfo.emplace_back(new UmiSample(
            i, info["name"].asString(), barcodes.size(),
            info["inlineUmi"].asBool(), info["acceptableUmiList"].asString(),
            info["patterns"]));
      } else {
        sampleinfo.emplace_back(
            new SimpleSample(i, info["name"].asString(), barcodes.size()));
      }

      for (Json::ArrayIndex b = 0; b < barcodes.size(); b++) {
        samplesheet_data << "sample" << i << "_" << b << ","
                         << "sample" << i << "_" << b << ",manifest,gf,";
        auto barcode = barcodes[b].asString();
        if (dual_barcoded) {
          auto dash_offset = barcode.find("-");
          auto i7 = barcode.substr(0, dash_offset);
          auto i5 = barcode.substr(dash_offset + 1, barcode.length());
          samplesheet_data << i7 << "," << i7 << "," << i5 << "," << i5;

        } else {
          samplesheet_data << barcode << "," << barcode;
        }
        samplesheet_data << "\n";
      }
    }
  }
  std::cerr << "Launching bcl2fastq..." << std::endl;

  // Set up the bcl2fastq command line and launch it.
  char *child_args[argc - optind + 5];
  for (auto i = optind; i < argc; i++) {
    child_args[i - optind] = argv[i];
  }
  // This is a nightmare of C memory management. We're just going to
  // duplicate and leak all these strings, because I don't care anymore.
  child_args[argc - optind] = strdup("--output-dir");
  child_args[argc - optind + 1] = temporary_directory;
  child_args[argc - optind + 2] = strdup("--sample-sheet");
  child_args[argc - optind + 3] = strdup(samplesheet.c_str());
  child_args[argc - optind + 4] = nullptr;

  // In UNIX, we can't create a new process from scratch; we duplicate the
  // existing process using fork() and the return value of fork() tells us
  // whether we are the parent (by giving us the child PID) or the child; the
  // child can then set itself up to be trace and execute bcl2fastq which
  // replaces its own state with that new program
  errno = 0;
  const auto pid = fork();
  if (pid < 0) {
    // Fork failed. Not much we can do
    perror("fork");
    return 1;
  } else if (pid == 0) {
    // This is the child process. Tell the OS we consent to be traced.
    errno = 0;
    if (ptrace(PTRACE_TRACEME, 0, 0, 0) < 0) {
      perror("fork");
      return 1;
    }
    // Send a stop signal to ourselves so we don't do anything until the parent
    // is ready to trace
    kill(getpid(), SIGSTOP);
    errno = 0;
    // Run bcl2fastq; this will replace this program's code with the child's,
    // but keep our ptrace settings and open files intact
    execvp(child_args[0], child_args);
    perror("execvp");
    return 1;
  } else {
    errno = 0;
    int status = 0;
    // Wait for the child to stop itself
    waitpid(pid, &status, 0);
    if (!WIFSTOPPED(status)) {
      std::cerr << "Child process got a signal that wasn't the STOP it was "
                   "supposed to send to itself."
                << std::endl;
      return 1;
    }
    errno = 0;
    // Tell the OS we are going to trace it and if we die, kill the child.
    if (ptrace(PTRACE_SETOPTIONS, pid, 0,
               PTRACE_O_TRACESYSGOOD | PTRACE_O_EXITKILL) < 0) {
      perror("ptrace");
      return 1;
    }
    // Keep checking on the child until it exists
    if (!run_process(pid)) {
      std::cerr << "Giving up." << std::endl;
      return 1;
    }
  }
  std::cerr << "Reading stats file..." << std::endl;
  Json::Value stats_json;
  std::ifstream stats_data(std::string(temporary_directory) +
                           "/Stats/Stats.json");
  stats_data >> stats_json;

  std::set<int> reads;
  for (Json::ArrayIndex i = 0; i < stats_json["ReadInfosForLanes"].size();
       i++) {
    auto &ril = stats_json["ReadInfosForLanes"][i];
    for (Json::ArrayIndex j = 0; j < ril["ReadInfos"].size(); j++) {
      auto &ri = ril["ReadInfos"][j];
      for (Json::ArrayIndex k = 0; k < ri.size(); k++) {
        reads.insert(ri["Number"].asInt());
      }
    }
  }

  // Find all the FASTQs that bcl2fastq has produced in the temporary
  // directory; the chosen ones will get copied out
  std::vector<std::string> fastqs;
  {
    DIR *dir = nullptr;
    errno = 0;
    dir = opendir(temporary_directory);
    if (dir == nullptr) {
      perror("opendir");
    } else {
      struct dirent *entry = nullptr;
      while ((entry = readdir(dir))) {

        if (strlen(entry->d_name) > strlen(".fastq.gz") &&
            strcmp(entry->d_name + strlen(entry->d_name) - strlen(".fastq.gz"),
                   ".fastq.gz") == 0) {
          fastqs.push_back(entry->d_name);
        }
      }
    }
    closedir(dir);
  }

  std::cerr << "Post processing data..." << std::endl;
  Json::Value output(Json::arrayValue);
  std::string outputDirectory(temporary_directory);
  std::vector<std::thread> running_threads;
  for (auto &sample : sampleinfo) {
    sample->process(running_threads, reads, outputDirectory, stats_json, output,
                    fastqs);
  }

  std::cerr << "Waiting on " << running_threads.size()
            << " concatenation threads to finish." << std::endl;
  for (auto &running_thread : running_threads) {
    if (running_thread.joinable()) {
      running_thread.join();
    } else {
      std::cerr << "Thread is not joinable. That shouldn't be possible."
                << std::endl;
    }
  }
  std::cerr << "Concatenation finished." << std::endl;

  Json::Value output_obj(Json::objectValue);
  output_obj["outputs"] = std::move(output);

  std::ofstream output_data("outputs.json");
  output_data << output_obj;

  return 0;
}
