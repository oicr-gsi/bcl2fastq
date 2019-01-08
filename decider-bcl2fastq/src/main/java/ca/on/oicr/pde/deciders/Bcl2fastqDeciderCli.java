package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.AnalysisProvenanceProvider;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.LaneProvenanceProvider;
import ca.on.oicr.gsi.provenance.MultiThreadedDefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.ProviderLoader;
import ca.on.oicr.gsi.provenance.SampleProvenanceProvider;
import ca.on.oicr.pde.deciders.configuration.StudyToOutputPathConfig;
import ca.on.oicr.pde.deciders.data.BasesMask;
import ca.on.oicr.pde.deciders.exceptions.InvalidBasesMaskException;
import ca.on.oicr.pde.deciders.utils.PineryClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.seqware.pipeline.plugins.WorkflowScheduler;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import net.sourceforge.seqware.common.model.Workflow;
import net.sourceforge.seqware.common.module.ReturnValue;
import net.sourceforge.seqware.common.util.filetools.FileTools;
import net.sourceforge.seqware.pipeline.decider.DeciderInterface;
import net.sourceforge.seqware.pipeline.plugin.Plugin;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Bcl2fastqDeciderCli extends Plugin implements DeciderInterface {

    private final Logger log = LogManager.getLogger(Bcl2fastqDeciderCli.class);

    private final OptionSpec<Boolean> ignorePreviousLimsKeysOpt;
    private final OptionSpec<Boolean> noNullCreatedDateOpt;
    private final OptionSpec<Boolean> disableRunCompleteCheckOpt;
    private final OptionSpec<Boolean> dryRunOpt;
    private final OptionSpec<Boolean> demultiplexSingleSampleModeOpt;
    private final OptionSpec<Boolean> createIusLimsKeysOpt;
    private final OptionSpec<Boolean> scheduleWorkflowRunsOpt;
    private final OptionSpec<Boolean> noMetadataOpt;
    private final OptionSpec<Integer> launchMaxOpt;
    private final OptionSpec<String> hostOpt;
    private final OptionSpec<Integer> wfSwidOpt;
    private final OptionSpec<Boolean> ignorePreviousRunsOpt;
    private final OptionSpec<String> checkWfSwidsOpts;
    private final OptionSpec helpOpt;
    private final OptionSpec<Boolean> verboseOpt;
    private final OptionSpec<String> outputPathOpt;
    private final OptionSpec<String> outputFolderOpt;
    private final OptionSpec<String> studyOutputPathOpt;
    private final OptionSpec<String> provenanceSettingsOpt;
    private final OptionSpec<String> pineryUrlOpt;
    private final OptionSpec allOpt;
    private final EnumMap<FileProvenanceFilter, OptionSpec<String>> includeFilterOpts;
    private final EnumMap<FileProvenanceFilter, OptionSpec<String>> excludeFilterOpts;
    private final OptionSpec<String> afterDateOpt;
    private final OptionSpec<String> beforeDateOpt;
    private final OptionSpec<String> includeInstrumentFilterOpt;
    private final OptionSpec<String> excludeInstrumentFilterOpt;
    private final OptionSpec<String> overrideRunBasesMaskOpt;
    private final OptionSpec<Integer> minAllowedEditDistanceOpt;
    private final NonOptionArgumentSpec<String> nonOptionSpec;
    private final Bcl2fastqDecider decider;
    private final OptionSpec<Boolean> noLaneSplittingOpt;
    private final OptionSpec<Boolean> ignoreLaneSkipOpt;

    public Bcl2fastqDeciderCli() {
        super();
        decider = new Bcl2fastqDecider();
        parser = new OptionParser();

        helpOpt = parser.acceptsAll(Arrays.asList("help", "h"), "Prints this help message").forHelp();
        verboseOpt = parser.accepts("verbose",
                "Enable debug logging")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);

        //provenance options
        provenanceSettingsOpt = parser.acceptsAll(Arrays.asList("provenance-settings"),
                "Path to provenance settings file.")
                .withRequiredArg().ofType(String.class).required();

        //seqware options
        wfSwidOpt = parser.acceptsAll(Arrays.asList("wf-accession"),
                "The workflow accession of the workflow.")
                .withRequiredArg().ofType(Integer.class).required();
        checkWfSwidsOpts = parser.acceptsAll(Arrays.asList("check-wf-accessions"),
                "The comma-separated, no spaces, workflow accessions of the workflow "
                + "that perform the same function (e.g. older versions). Any files "
                + "that have been processed with these workflows will be skipped.")
                .withRequiredArg().ofType(String.class);
        hostOpt = parser.acceptsAll(Arrays.asList("host", "ho"),
                "Used only in combination with --schedule to schedule onto a specific "
                + "host. If not provided, the default is the local host.")
                .withRequiredArg().ofType(String.class).defaultsTo(FileTools.getLocalhost(options).hostname);

        //options that modify the operation of the decider
        dryRunOpt = parser.acceptsAll(Arrays.asList("dry-run", "test"),
                "Dry-run/test mode. Prints the INI files to standard out and does not submit the workflow.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        demultiplexSingleSampleModeOpt = parser.acceptsAll(Arrays.asList("demux-single-sample"),
                "Demultiplex single sample rather than OICR default of NoIndex mode.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        createIusLimsKeysOpt = parser.acceptsAll(Arrays.asList("create-ius-lims-keys"),
                "Enable or disable the creation of IUS-LimsKeys objects in the SeqWare db (--dry-run/--test overrides this option).")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        scheduleWorkflowRunsOpt = parser.acceptsAll(Arrays.asList("schedule-workflow-runs"),
                "Enable or disable the scheduling of workflow runs (--dry-run/--test overrides this option).")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        noMetadataOpt = parser.acceptsAll(Arrays.asList("no-meta-db", "no-metadata"),
                "Prevents metadata writeback (which is done "
                + "by default) by the Decider and that is subsequently "
                + "passed to the called workflow which can use it to determine if "
                + "they should write metadata at runtime on the cluster.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        ignorePreviousRunsOpt = parser.acceptsAll(Arrays.asList("ignore-previous-runs", "force-run-all"),
                "Forces the decider to run all matches regardless of whether they've been run before or not.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        disableRunCompleteCheckOpt = parser.accepts("disable-run-complete-check",
                "Disable checking Pinery that the run is complete.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        ignorePreviousLimsKeysOpt = parser.accepts("ignore-previous-lims-keys",
                "Ignore all existing analysis (workflow runs and IUS skip).")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        launchMaxOpt = parser.acceptsAll(Arrays.asList("launch-max"),
                "The maximum number of jobs to launch at once.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(decider.getLaunchMax());
        noLaneSplittingOpt = parser.accepts("no-lane-splitting",
                "Schedule workflow runs using no-lane-splitting "
                + "(Note: this mode requires all lanes for a run be assigned the same samples or only lane 1 be assigned samples).")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        ignoreLaneSkipOpt = parser.accepts("ignore-lane-skip",
                "Ignore lane skip field and process lanes that are marked as skipped.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);

        //output options
        outputPathOpt = parser.accepts("output-path",
                "The absolute path of the directory to put the final file(s) (workflow output-prefix option).")
                .withRequiredArg().ofType(String.class).defaultsTo(decider.getOutputPath());
        outputFolderOpt = parser.accepts("output-folder",
                "The relative path to put the final result(s) (workflow output-dir option).")
                .withRequiredArg().ofType(String.class).defaultsTo(decider.getOutputFolder());
        studyOutputPathOpt = parser.accepts("study-to-output-path-csv",
                "The absolulte path to the \"Study To Output Path\" CSV file that defines "
                + "the workflow \"output-prefix\" (where the workflow output files should be written to).")
                .withRequiredArg().ofType(String.class);

        //filters options
        noNullCreatedDateOpt = parser.accepts(
                "no-null-created-date",
                "Set the filter comparison date to \"last modified\" date if \"created date\" is null.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        allOpt = parser.accepts("all",
                "Operate across everything (no filters)");
        afterDateOpt = parser.accepts("after-date",
                "Format YYYY-MM-DD. Only run on files that have been modified after a certain date, not inclusive.")
                .withRequiredArg();
        beforeDateOpt = parser.accepts("before-date",
                "Format YYYY-MM-DD. Only run on files that have been modified before a certain date, not inclusive.")
                .withRequiredArg();
        includeInstrumentFilterOpt = parser.accepts("include-instrument",
                "Include lanes were sequenced with instrument (\"instrument_name\" sequencer run attribute).")
                .withRequiredArg();
        excludeInstrumentFilterOpt = parser.accepts("exclude-instrument",
                "Exclude lanes were sequenced with instrument (\"instrument_name\" sequencer run attribute).")
                .withRequiredArg();

        //pinery client options (required if doing runCompleteCheck)
        pineryUrlOpt = parser.acceptsAll(Arrays.asList("pinery-url"), "URL to Pinery service.")
                .requiredUnless(disableRunCompleteCheckOpt).withRequiredArg().ofType(String.class);

        includeFilterOpts = new EnumMap<>(FileProvenanceFilter.class);
        excludeFilterOpts = new EnumMap<>(FileProvenanceFilter.class);
        for (FileProvenanceFilter filter : Bcl2fastqDecider.getSupportedFilters()) {
            includeFilterOpts.put(filter, parser.accepts("include-" + filter.toString()).withRequiredArg().ofType(String.class).withValuesSeparatedBy(","));
            excludeFilterOpts.put(filter, parser.accepts("exclude-" + filter.toString()).withRequiredArg().ofType(String.class).withValuesSeparatedBy(","));
        }

        overrideRunBasesMaskOpt = parser.accepts("override-run-bases-mask", "Override the run bases-mask and truncate barcodes to the specified index length.").withRequiredArg();

        minAllowedEditDistanceOpt = parser.accepts("min-allowed-edit-distance",
                "The minimum allowed barcode edit distance for sample barcodes within a lane (Default = " + decider.getMinAllowedEditDistance().toString() + ").").withRequiredArg().ofType(Integer.class);

        nonOptionSpec = parser.nonOptions(WorkflowScheduler.OVERRIDE_INI_DESC);
    }

    @Override
    public ReturnValue parse_parameters() {
        ReturnValue ret = new ReturnValue(ReturnValue.SUCCESS);
        try {
            options = parser.parse(params);
        } catch (OptionException e) {
            log.error(e.getMessage());
            ret.setExitStatus(ReturnValue.INVALIDARGUMENT);
        }
        return ret;
    }

    @Override
    public ReturnValue init() {
        ReturnValue rv = new ReturnValue(ReturnValue.SUCCESS);

        decider.setMetadata(metadata);
        decider.setConfig(config);

        if (getBooleanFlagOrArgValue(verboseOpt)) {
            //log4j logging configuration
            org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("ca.on.oicr");
            logger.setLevel(Level.DEBUG);
            logger.removeAllAppenders();
            logger.addAppender(new ConsoleAppender(new PatternLayout("%p [%d{yyyy/MM/dd HH:mm:ss}] | %m%n")));

            //log4j2 logging configuration
            Configurator.setRootLevel(org.apache.logging.log4j.Level.INFO);
            Configurator.setLevel("ca.on.oicr", org.apache.logging.log4j.Level.DEBUG);
        }

        if (options.has(helpOpt)) {
            get_syntax();
            rv.setExitStatus(ReturnValue.RETURNEDHELPMSG);
            return rv;
        }

        if (options.has(provenanceSettingsOpt)) {
            ProviderLoader providerLoader;
            try {
                providerLoader = new ProviderLoader(Paths.get(options.valueOf(provenanceSettingsOpt)));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            MultiThreadedDefaultProvenanceClient provenanceClientImpl = new MultiThreadedDefaultProvenanceClient();
            for (Entry<String, AnalysisProvenanceProvider> e : providerLoader.getAnalysisProvenanceProviders().entrySet()) {
                provenanceClientImpl.registerAnalysisProvenanceProvider(e.getKey(), e.getValue());
            }
            for (Entry<String, LaneProvenanceProvider> e : providerLoader.getLaneProvenanceProviders().entrySet()) {
                provenanceClientImpl.registerLaneProvenanceProvider(e.getKey(), e.getValue());
            }
            for (Entry<String, SampleProvenanceProvider> e : providerLoader.getSampleProvenanceProviders().entrySet()) {
                provenanceClientImpl.registerSampleProvenanceProvider(e.getKey(), e.getValue());
            }

            decider.setProvenanceClient(provenanceClientImpl);
        }

        if (options.has(pineryUrlOpt)) {
            PineryClient c = new PineryClient(options.valueOf(pineryUrlOpt));
            decider.setPineryClient(c);
        }

        Workflow workflow = metadata.getWorkflow(options.valueOf(wfSwidOpt));
        if (workflow == null) {
            throw new RuntimeException("Unable to find workflow swid = [" + options.valueOf(wfSwidOpt) + "]");
        }
        decider.setWorkflow(workflow);
        if (options.has(checkWfSwidsOpts)) {
            decider.setWorkflowAccessionsToCheck(Sets.newHashSet(options.valueOf(checkWfSwidsOpts).split(",")));
        }
        decider.setHost(options.valueOf(hostOpt));

        decider.setDoCreateIusLimsKeys(getBooleanFlagOrArgValue(createIusLimsKeysOpt));
        decider.setDoScheduleWorkflowRuns(getBooleanFlagOrArgValue(scheduleWorkflowRunsOpt));
        if (getBooleanFlagOrArgValue(dryRunOpt) || getBooleanFlagOrArgValue(noMetadataOpt)) {
            decider.setIsDryRunMode(true);
            decider.setDoMetadataWriteback(false);
            decider.setDoCreateIusLimsKeys(false);
            decider.setDoScheduleWorkflowRuns(false);
        } else {
            decider.setIsDryRunMode(false);
        }

        //allow user to disable NoIndex mode, incase a single sample from a multi-sample lane is being run
        decider.setIsDemultiplexSingleSampleMode(getBooleanFlagOrArgValue(demultiplexSingleSampleModeOpt));

        decider.setIgnorePreviousAnalysisMode(getBooleanFlagOrArgValue(ignorePreviousRunsOpt));
        decider.setIgnorePreviousLimsKeysMode(getBooleanFlagOrArgValue(ignorePreviousLimsKeysOpt));
        decider.setDisableRunCompleteCheck(getBooleanFlagOrArgValue(disableRunCompleteCheckOpt));
        decider.setLaunchMax(options.valueOf(launchMaxOpt));
        decider.setNoLaneSplittingMode(getBooleanFlagOrArgValue(noLaneSplittingOpt));
        decider.setIgnoreLaneSkip(getBooleanFlagOrArgValue(ignoreLaneSkipOpt));

        decider.setOutputPath(options.valueOf(outputPathOpt).endsWith("/") ? options.valueOf(outputPathOpt) : options.valueOf(outputPathOpt) + "/");
        decider.setOutputFolder(options.valueOf(outputFolderOpt));
        if (options.has(studyOutputPathOpt)) {
            if (options.has(outputPathOpt)) {
                log.error("Use {} or {} - not both", studyOutputPathOpt.toString(), outputPathOpt.toString());
                rv.setExitStatus(ReturnValue.INVALIDPARAMETERS);
            }
            try {
                decider.setStudyToOutputPathConfig(new StudyToOutputPathConfig(options.valueOf(studyOutputPathOpt)));
            } catch (IOException ex) {
                log.error("{} is not accessible", studyOutputPathOpt.toString());
                rv.setExitStatus(ReturnValue.INVALIDPARAMETERS);
            }
        }

        decider.setReplaceNullCreatedDate(getBooleanFlagOrArgValue(noNullCreatedDateOpt));

        Function<String, Set<String>> function = (String input) -> {
            if (input.startsWith("file://") || input.startsWith("/")) {
                Path filePath;
                if (input.startsWith("file://")) {
                    filePath = Paths.get(URI.create(input));
                } else if (input.startsWith("/")) {
                    filePath = Paths.get(input);
                } else {
                    throw new IllegalArgumentException("Unsupported input file path");
                }
                try {
                    Stream<String> lines = Files.lines(filePath);
                    return lines.filter(line -> !line.isEmpty()).collect(Collectors.toSet());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                return Stream.of(input).collect(Collectors.toSet());
            }
        };

        EnumMap<FileProvenanceFilter, Set<String>> includeFilters = new EnumMap<>(FileProvenanceFilter.class);
        for (Entry<FileProvenanceFilter, OptionSpec<String>> e : includeFilterOpts.entrySet()) {
            if (options.has(e.getValue())) {
                Set<String> vals = options.valuesOf(e.getValue()).stream().map(function).flatMap(m -> m.stream()).collect(Collectors.toSet());
                includeFilters.put(e.getKey(), ImmutableSet.copyOf(vals));
            }
        }
        decider.setIncludeFilters(includeFilters);

        EnumMap<FileProvenanceFilter, Set<String>> excludeFilters = new EnumMap<>(FileProvenanceFilter.class);
        for (Entry<FileProvenanceFilter, OptionSpec<String>> e : excludeFilterOpts.entrySet()) {
            if (options.has(e.getValue())) {
                Set<String> vals = options.valuesOf(e.getValue()).stream().map(function).flatMap(m -> m.stream()).collect(Collectors.toSet());
                excludeFilters.put(e.getKey(), ImmutableSet.copyOf(vals));
            }
        }
        decider.setExcludeFilters(excludeFilters);

        if (!(options.has(allOpt) ^ (!includeFilters.isEmpty() || !excludeFilters.isEmpty()))) {
            log.error("--all or a combination of the following include/exclude filters should be specified [{}]",
                    Joiner.on(",").join(Bcl2fastqDecider.getSupportedFilters()));
            rv.setExitStatus(ReturnValue.INVALIDPARAMETERS);
        }

        DateTimeFormatter format = DateTimeFormatter.ISO_LOCAL_DATE;
        if (options.has(afterDateOpt)) {
            try {
                decider.setAfterDateFilter(LocalDate.parse(options.valueOf(afterDateOpt), format).atStartOfDay(ZoneOffset.UTC));
            } catch (DateTimeParseException e) {
                log.error("After Date should be in the format: " + format.toString(), e);
                rv.setExitStatus(ReturnValue.INVALIDPARAMETERS);
            }
        }
        if (options.has(beforeDateOpt)) {
            try {
                decider.setBeforeDateFilter(LocalDate.parse(options.valueOf(beforeDateOpt), format).atStartOfDay(ZoneOffset.UTC));
            } catch (DateTimeParseException e) {
                log.error("Before Date should be in the format: " + format.toString(), e);
                rv.setExitStatus(ReturnValue.INVALIDPARAMETERS);
            }
        }
        if (options.has(includeInstrumentFilterOpt)) {
            decider.setIncludeInstrumentNameFilter(ImmutableSet.copyOf(options.valuesOf(includeInstrumentFilterOpt)));
        }
        if (options.has(excludeInstrumentFilterOpt)) {
            decider.setExcludeInstrumentNameFilter(ImmutableSet.copyOf(options.valuesOf(excludeInstrumentFilterOpt)));
        }

        if (options.has(overrideRunBasesMaskOpt)) {
            try {
                decider.setOverrideRunBasesMask(BasesMask.fromString(options.valueOf(overrideRunBasesMaskOpt)));
            } catch (InvalidBasesMaskException ex) {
                log.error("Invalid override-run-bases-mask string: ", ex);
                rv.setExitStatus(ReturnValue.INVALIDPARAMETERS);
            }
        }

        if (options.has(minAllowedEditDistanceOpt)) {
            decider.setMinAllowedEditDistance(options.valueOf(minAllowedEditDistanceOpt));
        }

        decider.setOverrides(options.valuesOf(nonOptionSpec));

        return rv;
    }

    private boolean getBooleanFlagOrArgValue(OptionSpec<Boolean> param) {
        if (options.has(param)) {
            if (options.hasArgument(param)) {
                //return explicit boolean provided as arg
                return options.valueOf(param);
            } else {
                //only parameter provided - treat as flag
                return true;
            }
        } else {
            //return default
            return options.valueOf(param);
        }
    }

    @Override
    public ReturnValue do_run() {
        //run the decider
        try {
            decider.run();
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
            return new ReturnValue(ReturnValue.ExitStatus.RUNNERERR);
        }

        //calculate exit code
        if (!decider.getInvalidLanes().isEmpty()) {
            //return exit code 91
            return new ReturnValue(ReturnValue.ExitStatus.RUNNERERR);
        } else if (!decider.getValidWorkflowRuns().isEmpty() && decider.getValidWorkflowRuns().size() < decider.getScheduledWorkflowRuns().size()) {
            //return exit code 101
            return new ReturnValue(ReturnValue.ExitStatus.QUEUED);
        } else {
            return new ReturnValue(ReturnValue.ExitStatus.SUCCESS);
        }
    }

    @Override
    public ReturnValue do_summary() {
        return new ReturnValue(ReturnValue.ExitStatus.SUCCESS);
    }

    @Override
    public ReturnValue do_test() {
        return new ReturnValue(ReturnValue.ExitStatus.SUCCESS);
    }

    @Override
    public ReturnValue clean_up() {
        return new ReturnValue(ReturnValue.ExitStatus.SUCCESS);
    }

    public Bcl2fastqDecider getBcl2fastqDecider() {
        return decider;
    }

    public static void main(String args[]) {
        List<String> params = new ArrayList<>();
        params.add("--plugin");
        params.add(Bcl2fastqDeciderCli.class.getCanonicalName());
        params.add("--");
        params.addAll(Arrays.asList(args));
        System.out.println("Parameters: " + Arrays.deepToString(params.toArray()));
        net.sourceforge.seqware.pipeline.runner.PluginRunner.main(params.toArray(new String[params.size()]));
    }

}
