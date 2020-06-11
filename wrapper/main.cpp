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
#include <iostream>
#include <json/json.h>
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

// Store information about a sample in the input (might map to multiple rows in
// the sample sheet if multiple barcodes)
class Sample {
public:
  Sample(Json::ArrayIndex id_, const std::string &name_, int barcodes_)
      : id(id_), barcodes(barcodes_), name(name_) {}

  // Find any matching files and spin up a concatenation thread for each one;
  // find the read count and write out JSON data with the output files and read
  // count
  void process(std::vector<std::thread> &threads, std::set<int> &reads,
               const std::string &outputDir, Json::Value &stats_json,
               Json::Value &output, std::vector<std::string> &fastqs) {

    std::cerr << "Starting for " << name << "..." << std::endl;

    auto readCount = 0;
    // For each of the input barcodes, pick up the matching read count
    for (auto barcode = 0; barcode < barcodes; barcode++) {
      std::stringstream targetName;
      auto found = false;
      targetName << "sample" << id << "_" << barcode;
      for (Json::ArrayIndex i = 0;
           !found && i < stats_json["ConversionResults"].size(); i++) {
        auto &cr = stats_json["ConversionResults"][i];
        for (Json::ArrayIndex j = 0; !found && j < cr["DemuxResults"].size();
             j++) {
          auto &dr = cr["DemuxResults"][j];
          for (Json::ArrayIndex k = 0; k < dr.size(); k++) {
            if (dr["SampleName"].asString() == targetName.str()) {
              readCount += dr["NumberReads"].asInt();
              found = true;
            }
          }
        }
      }
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
        std::cerr << "Meh. It's some other file." << std::endl;
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
  while (true) {
    int status = 0;
    errno = 0;
    if (ptrace(PTRACE_SYSCALL, child, 0, 0) < 0) {
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
    /* Is it a syscall we care about? */
    if (WIFSTOPPED(status) && WSTOPSIG(status) & 0x80) {
      switch (ptrace(PTRACE_PEEKUSER, child, sizeof(long) * ORIG_RAX, 0)) {
      // When it tries to open a file, intervene.
      case 2: // open()
        static_assert(
            sizeof(devnull) % sizeof(long) == 0,
            "undetermined replacement path is not a multiple of long");
        std::cerr << "Investigating open() system call" << std::endl;
        conditional_rewrite(child, RDI, "Undetermined", devnull,
                            sizeof(devnull));
        break;
      case 257: // openat()
        std::cerr << "Investigating openat() system call" << std::endl;
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
        std::cerr << "Investigating stat()/lstat() system call" << std::endl;
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
      // Our process may be dead, so do a wait, then try the ptrace(SYSCALL) if
      // it lived
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
  std::vector<Sample> sampleinfo;
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
      auto &barcodes = samples_json["samples"][i]["barcodes"];
      sampleinfo.emplace_back(i, samples_json["samples"][i]["name"].asString(),
                              barcodes.size());

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
  for (Json::ArrayIndex i = 0; i < stats_json["ConversionResults"].size();
       i++) {
    auto &cr = stats_json["ConversionResults"][i];
    for (Json::ArrayIndex j = 0; j < cr["Undetermined"]["ReadMetrics"].size();
         j++) {
      auto &rm = cr["Undetermined"]["ReadMetrics"][j];
      for (Json::ArrayIndex k = 0; k < rm.size(); k++) {
        reads.insert(rm["ReadNumber"].asInt());
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
    sample.process(running_threads, reads, outputDirectory, stats_json, output,
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
