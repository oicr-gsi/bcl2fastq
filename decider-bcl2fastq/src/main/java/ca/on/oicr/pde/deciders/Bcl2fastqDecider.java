package ca.on.oicr.pde.deciders;

import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;
import ca.on.oicr.pde.deciders.data.ProvenanceWithProvider;
import ca.on.oicr.gsi.provenance.ExtendedProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.configuration.StudyToOutputPathConfig;
import ca.on.oicr.pde.deciders.data.Barcode;
import ca.on.oicr.pde.deciders.data.BarcodeCollision;
import ca.on.oicr.pde.deciders.handlers.Bcl2Fastq1Handler;
import ca.on.oicr.pde.deciders.handlers.Bcl2Fastq2Handler;
import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import ca.on.oicr.pde.deciders.handlers.Bcl2FastqHandler;
import ca.on.oicr.pde.deciders.handlers.Handler;
import ca.on.oicr.pde.deciders.data.BasesMask;
import ca.on.oicr.pde.deciders.data.SampleProvenanceWithCustomBarcode;
import ca.on.oicr.pde.deciders.exceptions.InvalidLaneException;
import ca.on.oicr.pde.deciders.utils.BarcodeComparison;
import ca.on.oicr.pde.deciders.utils.BarcodeAndBasesMask;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import net.sourceforge.seqware.common.metadata.Metadata;
import net.sourceforge.seqware.common.model.Workflow;
import net.sourceforge.seqware.pipeline.runner.PluginRunner;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2fastqDecider {

    private final Logger log = LogManager.getLogger(Bcl2fastqDecider.class);
    private final List<Bcl2FastqHandler> handlers = new ArrayList<>();

    private ExtendedProvenanceClient provenanceClient;
    private Metadata metadata;
    private Map<String, String> config;
    private Workflow workflow;
    private Integer launchMax = 10;
    private Set<String> workflowAccessionsToCheck = Collections.EMPTY_SET;
    private String host;

    private Boolean isDryRunMode = false;
    private Boolean doMetadataWriteback = true;
    private Boolean doCreateIusLimsKeys = true;
    private Boolean doScheduleWorkflowRuns = true;
    private Boolean ignorePreviousAnalysisMode = false;
    private Boolean ignorePreviousLimsKeysMode = false;
    private Boolean disableRunCompleteCheck = false;
    private Boolean isDemultiplexSingleSampleMode = false;

    private String outputPath = "./";
    private String outputFolder = "seqware-results";
    private StudyToOutputPathConfig studyToOutputPathConfig;

    private Boolean replaceNullCreatedDate = false;
    private ZonedDateTime afterDateFilter = null;
    private ZonedDateTime beforeDateFilter = null;
    private Set<String> includeInstrumentNameFilter;
    private Set<String> excludeInstrumentNameFilter;
    private EnumMap<FileProvenanceFilter, Set<String>> includeFilters = new EnumMap<>(FileProvenanceFilter.class);
    private EnumMap<FileProvenanceFilter, Set<String>> excludeFilters = new EnumMap<>(FileProvenanceFilter.class);

    private BasesMask overrideBasesMask;

    private List<String> overrides;

    private final List<WorkflowRun> validWorkflowRuns = new ArrayList<>();
    private final List<WorkflowRun> scheduledWorkflowRuns = new ArrayList<>();
    private final List<String> laneErrors = new ArrayList<>();

    private Integer minAllowedEditDistance = 3;

    public Bcl2fastqDecider() {
        //add workflow handlers
        handlers.add(new Bcl2Fastq1Handler()); //CASAVA 2.7 handler
        handlers.add(new Bcl2Fastq2Handler()); //CASAVA 2.8+ handler
    }

    public static Set<FileProvenanceFilter> getSupportedFilters() {
        return ImmutableSet.of(
                FileProvenanceFilter.lane,
                FileProvenanceFilter.sequencer_run,
                FileProvenanceFilter.study,
                FileProvenanceFilter.sequencer_run_platform_model,
                FileProvenanceFilter.sample
        );
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Boolean getIsDryRunMode() {
        return isDryRunMode;
    }

    public void setIsDryRunMode(Boolean isDryRunMode) {
        this.isDryRunMode = isDryRunMode;
    }

    public Boolean getIsDemultiplexSingleSampleMode() {
        return isDemultiplexSingleSampleMode;
    }

    public void setIsDemultiplexSingleSampleMode(Boolean isDemultiplexSingleSampleMode) {
        this.isDemultiplexSingleSampleMode = isDemultiplexSingleSampleMode;
    }

    public Boolean getDoMetadataWriteback() {
        return doMetadataWriteback;
    }

    public void setDoMetadataWriteback(Boolean doMetadataWriteback) {
        this.doMetadataWriteback = doMetadataWriteback;
    }

    public Boolean getDoCreateIusLimsKeys() {
        return doCreateIusLimsKeys;
    }

    public void setDoCreateIusLimsKeys(Boolean doCreateIusLimsKeys) {
        this.doCreateIusLimsKeys = doCreateIusLimsKeys;
    }

    public Boolean getDoScheduleWorkflowRuns() {
        return doScheduleWorkflowRuns;
    }

    public void setDoScheduleWorkflowRuns(Boolean doScheduleWorkflowRuns) {
        this.doScheduleWorkflowRuns = doScheduleWorkflowRuns;
    }

    public Set<String> getWorkflowAccessionsToCheck() {
        return workflowAccessionsToCheck;
    }

    public void setWorkflowAccessionsToCheck(Set<String> workflowAccessionsToCheck) {
        this.workflowAccessionsToCheck = workflowAccessionsToCheck;
    }

    public Integer getLaunchMax() {
        return launchMax;
    }

    public void setLaunchMax(Integer launchMax) {
        this.launchMax = launchMax;
    }

    public boolean isIgnorePreviousAnalysisMode() {
        return ignorePreviousAnalysisMode;
    }

    public void setIgnorePreviousAnalysisMode(boolean ignorePreviousAnalysisMode) {
        this.ignorePreviousAnalysisMode = ignorePreviousAnalysisMode;
    }

    public boolean isIgnorePreviousLimsKeysMode() {
        return ignorePreviousLimsKeysMode;
    }

    public void setIgnorePreviousLimsKeysMode(boolean ignorePreviousLimsKeysMode) {
        this.ignorePreviousLimsKeysMode = ignorePreviousLimsKeysMode;
    }

    public boolean isDisableRunCompleteCheck() {
        return disableRunCompleteCheck;
    }

    public void setDisableRunCompleteCheck(boolean disableRunCompleteCheck) {
        this.disableRunCompleteCheck = disableRunCompleteCheck;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public StudyToOutputPathConfig getStudyToOutputPathConfig() {
        return studyToOutputPathConfig;
    }

    public void setStudyToOutputPathConfig(StudyToOutputPathConfig studyToOutputPathConfig) {
        this.studyToOutputPathConfig = studyToOutputPathConfig;
    }

    public boolean isReplaceNullCreatedDate() {
        return replaceNullCreatedDate;
    }

    public void setReplaceNullCreatedDate(boolean replaceNullCreatedDate) {
        this.replaceNullCreatedDate = replaceNullCreatedDate;
    }

    public ZonedDateTime getAfterDateFilter() {
        return afterDateFilter;
    }

    public void setAfterDateFilter(ZonedDateTime afterDateFilter) {
        this.afterDateFilter = afterDateFilter;
    }

    public ZonedDateTime getBeforeDateFilter() {
        return beforeDateFilter;
    }

    public void setBeforeDateFilter(ZonedDateTime beforeDateFilter) {
        this.beforeDateFilter = beforeDateFilter;
    }

    public Set<String> getIncludeInstrumentNameFilter() {
        return includeInstrumentNameFilter;
    }

    public void setIncludeInstrumentNameFilter(Set<String> includeInstrumentNameFilter) {
        this.includeInstrumentNameFilter = includeInstrumentNameFilter;
    }

    public Set<String> getExcludeInstrumentNameFilter() {
        return excludeInstrumentNameFilter;
    }

    public void setExcludeInstrumentNameFilter(Set<String> excludeInstrumentNameFilter) {
        this.excludeInstrumentNameFilter = excludeInstrumentNameFilter;
    }

    public List<String> getOverrides() {
        return overrides;
    }

    public void setOverrides(List<String> overrides) {
        this.overrides = overrides;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public EnumMap<FileProvenanceFilter, Set<String>> getIncludeFilters() {
        return includeFilters;
    }

    public void setIncludeFilters(EnumMap<FileProvenanceFilter, Set<String>> includeFilters) {
        this.includeFilters = includeFilters;
    }

    public EnumMap<FileProvenanceFilter, Set<String>> getExcludeFilters() {
        return excludeFilters;
    }

    public void setExcludeFilters(EnumMap<FileProvenanceFilter, Set<String>> excludeFilters) {
        this.excludeFilters = excludeFilters;
    }

    public BasesMask getOverrideBasesMask() {
        return this.overrideBasesMask;
    }

    public void setOverrideBasesMask(BasesMask basesMask) {
        this.overrideBasesMask = basesMask;
    }

    public Integer getMinAllowedEditDistance() {
        return minAllowedEditDistance;
    }

    public void setMinAllowedEditDistance(Integer minAllowedEditDistance) {
        this.minAllowedEditDistance = minAllowedEditDistance;
    }

    public void setProvenanceClient(ExtendedProvenanceClient provenanceClient) {
        this.provenanceClient = provenanceClient;
    }

    public void setWorkflow(Workflow workflow) {
        this.workflow = workflow;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public List<WorkflowRun> getValidWorkflowRuns() {
        return validWorkflowRuns;
    }

    public List<WorkflowRun> getScheduledWorkflowRuns() {
        return scheduledWorkflowRuns;
    }

    public List<String> getInvalidLanes() {
        return laneErrors;
    }

    public List<WorkflowRun> run() {
        checkNotNull(workflow);

        //get workflow handler
        String workflowName = workflow.getName();
        String workflowVersion = workflow.getVersion();
        if (!hasHandler(workflowName, workflowVersion)) {
            throw new RuntimeException("Workflow [" + workflowName + "-" + workflowVersion + "] is not supported");
        }
        Bcl2FastqHandler handler = getHandler(workflowName, workflowVersion);

        //reset workflow run collections
        validWorkflowRuns.clear();
        laneErrors.clear();
        scheduledWorkflowRuns.clear();

        // provenance data structures
        ListMultimap<String, ProvenanceWithProvider<LaneProvenance>> laneNameToLaneProvenance = ArrayListMultimap.create();
        Map<String, String> providerAndIdToLaneName = new HashMap<>();

        // get all lane provenance from providers specified in provenance settings
        Map<String, Collection<LaneProvenance>> laneProvenanceByProvider = provenanceClient.getLaneProvenanceByProvider(Collections.EMPTY_MAP); //filters);
        for (Map.Entry<String, Collection<LaneProvenance>> e : laneProvenanceByProvider.entrySet()) {
            String provider = e.getKey();
            for (LaneProvenance lp : e.getValue()) {
                String laneName = lp.getSequencerRunName() + "_lane_" + lp.getLaneNumber();
                providerAndIdToLaneName.put(provider + lp.getProvenanceId(), laneName);

                if (lp.getSkip()) {
                    log.debug("Lane = [{}] is skipped", laneName);
                    continue;
                }

                ZonedDateTime createdDate;
                if (replaceNullCreatedDate && lp.getCreatedDate() == null) { //ignore created date it is null
                    createdDate = lp.getLastModified();
                } else {
                    createdDate = lp.getCreatedDate();
                }

                if (createdDate == null) {
                    log.warn("Lane = [{}] has a null created date - treating lane as incomplete", laneName);
                    continue;
                }

                if (afterDateFilter != null && createdDate.isBefore(afterDateFilter)) {
                    continue;
                }

                if (beforeDateFilter != null && createdDate.isAfter(beforeDateFilter)) {
                    continue;
                }

                if (includeFilters.containsKey(FileProvenanceFilter.sequencer_run)
                        && !includeFilters.get(FileProvenanceFilter.sequencer_run).contains(lp.getSequencerRunName())) {
                    continue;
                }

                if (includeFilters.containsKey(FileProvenanceFilter.lane)
                        && !includeFilters.get(FileProvenanceFilter.lane).contains(laneName)) {
                    continue;
                }

                if (includeFilters.containsKey(FileProvenanceFilter.sequencer_run_platform_model)
                        && !includeFilters.get(FileProvenanceFilter.sequencer_run_platform_model).contains(lp.getSequencerRunPlatformModel())) {
                    continue;
                }

                if (includeInstrumentNameFilter != null
                        && !CollectionUtils.containsAny(includeInstrumentNameFilter, lp.getSequencerRunAttributes().get("instrument_name"))) {
                    continue;
                }

                if (excludeFilters.containsKey(FileProvenanceFilter.sequencer_run)
                        && excludeFilters.get(FileProvenanceFilter.sequencer_run).contains(lp.getSequencerRunName())) {
                    continue;
                }

                if (excludeFilters.containsKey(FileProvenanceFilter.lane)
                        && excludeFilters.get(FileProvenanceFilter.lane).contains(laneName)) {
                    continue;
                }

                if (excludeFilters.containsKey(FileProvenanceFilter.sequencer_run_platform_model)
                        && excludeFilters.get(FileProvenanceFilter.sequencer_run_platform_model).contains(lp.getSequencerRunPlatformModel())) {
                    continue;
                }

                if (excludeInstrumentNameFilter != null
                        && CollectionUtils.containsAny(excludeInstrumentNameFilter, lp.getSequencerRunAttributes().get("instrument_name"))) {
                    continue;
                }

                laneNameToLaneProvenance.put(laneName, new ProvenanceWithProvider<>(provider, lp));
            }
        }

        // get all sample provenance from providers specified in provenance settings
        ListMultimap<String, ProvenanceWithProvider<SampleProvenance>> laneNameToSampleProvenance = ArrayListMultimap.create();
        SetMultimap<String, String> laneNameToStudyNames = HashMultimap.create();
        Map<String, Collection<SampleProvenance>> sampleProvenanceByProvider = provenanceClient.getSampleProvenanceByProvider(Collections.EMPTY_MAP);
        for (Map.Entry<String, Collection<SampleProvenance>> e : sampleProvenanceByProvider.entrySet()) {
            String provider = e.getKey();
            for (SampleProvenance sp : e.getValue()) {
                String laneName = sp.getSequencerRunName() + "_lane_" + sp.getLaneNumber();
                providerAndIdToLaneName.put(provider + sp.getProvenanceId(), laneName);

                if (sp.getSkip()) {
                    log.debug("Sample = [{}] in lane = [{}] is skipped", sp.getSampleName(), laneName);
                    continue;
                }

                if (includeFilters.containsKey(FileProvenanceFilter.sample)
                        && !includeFilters.get(FileProvenanceFilter.sample).contains(sp.getSampleName())) {
                    continue;
                }

                if (excludeFilters.containsKey(FileProvenanceFilter.sample)
                        && excludeFilters.get(FileProvenanceFilter.sample).contains(sp.getSampleName())) {
                    continue;
                }

                laneNameToStudyNames.put(laneName, sp.getStudyTitle());
                laneNameToSampleProvenance.put(laneName, new ProvenanceWithProvider<>(provider, sp));
            }
        }

        if (includeFilters.containsKey(FileProvenanceFilter.study)) {
            Set<String> lanesToRemove = new HashSet<>();
            Set<String> includeStudies = includeFilters.get(FileProvenanceFilter.study);

            for (Entry<String, Set<String>> e : Multimaps.asMap(laneNameToStudyNames).entrySet()) {
                String laneName = e.getKey();
                Set<String> laneStudies = e.getValue();

                //the set of studies in the lane that are not in the include studies list
                Set<String> additionalStudiesInLane = Sets.difference(laneStudies, includeStudies);

                if (additionalStudiesInLane.isEmpty()) {
                    //lane passed study filter
                } else {
                    log.debug("Lane = [{}] with studies = [{}] removed due to study filter", laneName, Joiner.on(",").join(laneStudies));
                    lanesToRemove.add(e.getKey());
                }
            }
            laneNameToLaneProvenance.keySet().removeAll(lanesToRemove);
        }

        if (excludeFilters.containsKey(FileProvenanceFilter.study)) {
            Set<String> lanesToRemove = new HashSet<>();
            Set<String> excludeStudies = excludeFilters.get(FileProvenanceFilter.study);

            for (Entry<String, Set<String>> e : Multimaps.asMap(laneNameToStudyNames).entrySet()) {
                String laneName = e.getKey();
                Set<String> laneStudies = e.getValue();

                //the set of studies in the lane that are in the exclude studies list
                Set<String> matches = Sets.intersection(laneStudies, excludeStudies);

                if (matches.isEmpty()) {
                    //lane passed study filter
                } else {
                    log.debug("Lane = [{}] with studies = [{}] removed due to study filter", laneName, Joiner.on(",").join(laneStudies));
                    lanesToRemove.add(e.getKey());
                }
            }
            laneNameToLaneProvenance.keySet().removeAll(lanesToRemove);
        }

        //get previous analysis
        Map<FileProvenanceFilter, Set<String>> analysisFilters = new HashMap<>();
        //TODO: seqware currently does not support retrieving FP with null workflow swids
        //Set<String> workflowSwidsToCheck = new HashSet<>();
        //workflowSwidsToCheck.add(getWorkflowAccession());
        //workflowSwidsToCheck.add(null);
        //workflowSwidsToCheck.addAll(getWorkflowAccessionsToCheck());
        //analysisFilters.put(FileProvenanceFilter.workflow, workflowSwidsToCheck);

        //the set of all lanes that have been analyzed using the current workflow or a workflow in the set of "check workflows"
        Set<String> analyzedLanes = new HashSet<>();

        //calculate the set of lanes that do not have associated workflow information but are linked in seqware
        Set<String> blockedLanes = new HashSet<>();

        if (ignorePreviousLimsKeysMode) {
            // ignorePreviousLimsKeysMode enabled, no need to download any analysis as all known lanes are candidate lanes for analysis
        } else {
            Collection<FileProvenance> fps = provenanceClient.getFileProvenance(analysisFilters);
            for (FileProvenance fp : fps) {
                if (fp.getWorkflowSWID() != null) { //analysis provenance for a worklow run
                    if (getWorkflowAccessionsToCheck().contains(fp.getWorkflowSWID().toString())
                            || workflow.getSwAccession().equals(fp.getWorkflowSWID())) {
                        analyzedLanes.addAll(fp.getLaneNames());
                    }
                } else { //analysis provenance for an ius
                    //include this record, it may be an IUS that is used for skipping a lane or sample
                    blockedLanes.addAll(fp.getLaneNames());
                }
            }
        }

        //calculate the set of all lanes that are known by the set of lane provenance providers
        Set<String> knownLanes = laneNameToLaneProvenance.keySet();

        //calculate the set of all lanes that are known and valid
        Set<String> allowedLanes = Sets.difference(knownLanes, blockedLanes);

        //calculate the set of lanes that have not been processed
        Set<String> unprocessedLanes = Sets.difference(allowedLanes, analyzedLanes);

        //filter candidate lanes into lanes to analyze
        Set<String> candidateLanesToAnalyze = new HashSet<>();
        if (ignorePreviousLimsKeysMode) {
            candidateLanesToAnalyze.addAll(knownLanes);
        } else if (ignorePreviousAnalysisMode) {
            candidateLanesToAnalyze.addAll(allowedLanes);
        } else {
            candidateLanesToAnalyze.addAll(unprocessedLanes);
        }

        //lane validation before scheduling workflow run
        Set<String> invalidLanes = new HashSet<>();
        for (String laneName : candidateLanesToAnalyze) {
            List<String> laneErrors = new ArrayList<>();

            //expect one and only one lane provenance per lane name
            List<ProvenanceWithProvider<LaneProvenance>> lps = laneNameToLaneProvenance.get(laneName);
            if (lps.size() != 1) {
                invalidLanes.add(laneName);
                laneErrors.add(String.format("Lane provenance count = [%s], expected 1.", lps.size()));
            }

            //expect one or more sample provenance per lane name
            List<ProvenanceWithProvider<SampleProvenance>> sps = laneNameToSampleProvenance.get(laneName);
            if (sps.isEmpty()) {
                invalidLanes.add(laneName);
                laneErrors.add(String.format("Sample provenance count = [%s], expected 1 or more.", sps.size()));
            }

            if (!disableRunCompleteCheck) {
                if (lps.size() == 1) {
                    Set<String> runDirs = Iterables.getOnlyElement(lps).getProvenance().getSequencerRunAttributes().get("run_dir");
                    if (runDirs != null && runDirs.size() == 1) {
                        Path runDirPath = Paths.get(Iterables.getOnlyElement(Iterables.getOnlyElement(lps).getProvenance().getSequencerRunAttributes().get("run_dir")));
                        File runDir = runDirPath.toFile();
                        if (runDir.exists() && runDir.isDirectory() && runDir.canRead()) {
                            File oicrRunCompleteTouchFile = runDirPath.resolve("oicr_run_complete").toFile();
                            if (oicrRunCompleteTouchFile.exists()) {
                                //run is complete
                            } else {
                                laneErrors.add(String.format("Lane has not completed sequencing ([%s] is missing).", oicrRunCompleteTouchFile.getAbsolutePath()));
                            }
                        } else {
                            laneErrors.add(String.format("Lane run_dir = [%s] is not accessible or does not exist.", runDir.getAbsolutePath()));
                        }
                    } else {
                        laneErrors.add(String.format("Lane run_dir = [%s].", (runDirs == null ? "" : Joiner.on(",").join(runDirs))));
                    }
                }
            }

            if (!laneErrors.isEmpty()) {
                invalidLanes.add(laneName);
                log.warn("Lane = [{}] can not be processed due to the following reasons:\n"
                        + "{}\n"
                        + "Lane provenance: [{}]\n"
                        + "Sample provenance: [{}]",
                        laneName, Joiner.on("\n").join(laneErrors), Joiner.on(";").join(lps), Joiner.on(";").join(sps));
            }
        }

        //remove invalid lanes from lanes to analyze set
        Set<String> lanesToAnalyze = Sets.difference(candidateLanesToAnalyze, invalidLanes);

        //collect required workflow run data - create IUS-LimsKey records in seqware that will be linked to the workflow run
        for (String laneName : lanesToAnalyze) {
            log.info("Processing lane [{}]", laneName);

            if (launchMax != null && validWorkflowRuns.size() >= launchMax) {
                log.info("Launch max [{}] reached.", launchMax);
                break;
            }

            // get samples in lane
            List<ProvenanceWithProvider<SampleProvenance>> samples = laneNameToSampleProvenance.get(laneName);

            //use overrideBasesMask or runBasesMask to calculate the sequenced barcodes
            if (overrideBasesMask != null) {
                try {
                    samples = applyBasesMask(samples, overrideBasesMask);
                } catch (DataMismatchException ex) {
                    addInvalidLane("Error while generating workflow run(s) for lane = [{0}], errors:\n{1}", laneName, ex.toString());
                    continue;
                }
            }

            //get all barcodes in lane
            Set<String> unsupportedBarcodeStrings = new HashSet<>();
            List<Barcode> laneBarcodes = samples.stream()
                    .map(ProvenanceWithProvider::getProvenance)
                    .map(SampleProvenance::getIusTag)
                    .map(barcodeString -> {
                        try {
                            return Barcode.fromString(barcodeString);
                        } catch (DataMismatchException ex) {
                            unsupportedBarcodeStrings.add(barcodeString);
                            return null;
                        }
                    })
                    .collect(Collectors.toList());
            if (!unsupportedBarcodeStrings.isEmpty()) {
                addInvalidLane("Error while generating workflow run(s) for lane = [{0}], unsupported barcode(s):\n{1}", laneName, unsupportedBarcodeStrings.toString());
                continue;
            }

            //check for barcode collisions
            List<BarcodeCollision> collisions = BarcodeComparison.getTruncatedHammingDistanceCollisions(laneBarcodes, minAllowedEditDistance);
            if (collisions.size() > 0) {
                addInvalidLane("Error while generating workflow run(s) for lane = [{0}], barcode collision(s):\n{1}", laneName, Joiner.on("\n").join(collisions));
                continue;
            }

            //more than one sample in the lane, demultiplexing must be done for all workflow runs scheduled for this lane
            boolean doDemulitplexing = getIsDemultiplexSingleSampleMode();
            if (samples.size() > 1) {
                doDemulitplexing = true;
            }

            //group lane samples by barcode length
            Map<String, List<ProvenanceWithProvider<SampleProvenance>>> samplesGroupedByBarcodeLength = samples.stream()
                    .collect(Collectors.groupingBy(s -> {
                        try {
                            return Barcode.fromString(s.getProvenance().getIusTag()).getLengthString();
                        } catch (DataMismatchException ex) {
                            //this error should have been handled already so this exception should not be thrown
                            throw new RuntimeException(ex);
                        }
                    }));

            //iterate over all lane samples grouped by barcode length
            try {
                validWorkflowRuns.addAll(generateWorkflowRunsForLane(Iterables.getOnlyElement(laneNameToLaneProvenance.get(laneName)),
                        samplesGroupedByBarcodeLength, handler, doDemulitplexing));
            } catch (InvalidLaneException ex) {
                addInvalidLane("Error while generating workflow run(s) for lane = [{0}]:\n{1}", laneName, ex.toString());
            }
        }

        //list invalid lanes
        for (String invalidLane : this.laneErrors) {
            log.error(invalidLane);
//            log.warn("Invalid workflow run:\n" + debugWorkflowRun(wr));
        }

        //schedule workflow runs
        for (WorkflowRun wr : validWorkflowRuns) {
            if (getDoScheduleWorkflowRuns() && !getIsDryRunMode()) {
                log.info("Scheduled workflow run:\n" + scheduleWorkflowRun(wr));
                scheduledWorkflowRuns.add(wr);
            } else {
                log.info("Dry run mode enabled - not scheduling:\n" + debugWorkflowRun(wr));
            }
        }

        log.info("Lane input summary: fetched={} analyzed={} blocked={} allowed={} unprocessed={}",
                knownLanes.size(), analyzedLanes.size(), blockedLanes.size(), allowedLanes.size(), unprocessedLanes.size());
        log.info("Lane summary: requested={} invalid={} valid={}",
                candidateLanesToAnalyze.size(), invalidLanes.size(), lanesToAnalyze.size());
        log.info("Final summary: candidate lanes={} invalid lanes={} valid workflow runs={} scheduled workflow runs={}",
                lanesToAnalyze.size(), getInvalidLanes().size(), validWorkflowRuns.size(), scheduledWorkflowRuns.size());

        return scheduledWorkflowRuns;
    }

    private String scheduleWorkflowRun(WorkflowRun wr) {
        return scheduleWorkflowRun(wr, true);
    }

    private String debugWorkflowRun(WorkflowRun wr) {
        return scheduleWorkflowRun(wr, false);
    }

    private String scheduleWorkflowRun(WorkflowRun wr, boolean doScheduling) {
        //write ini properties to file
        Path iniFilePath = writeWorkflowRunIniPropertiesToFile(wr);

        WorkflowSchedulerCommandBuilder cmdBuilder = new WorkflowSchedulerCommandBuilder(workflow.getSwAccession());
        cmdBuilder.setIniFile(iniFilePath);
        cmdBuilder.setMetadataWriteback(getDoMetadataWriteback());
        cmdBuilder.setHost(host);
        cmdBuilder.setIusSwidsToLinkWorkflowRunTo(wr.getIusSwidsToLinkWorkflowRunTo());
        cmdBuilder.setOverrideArgs(overrides);
        List<String> runArgs = cmdBuilder.build();

        if (doScheduling) {
            PluginRunner pluginRunner = new PluginRunner();
            pluginRunner.setConfig(config);
            pluginRunner.run(runArgs.toArray(new String[runArgs.size()]));
        }

        return "Command: " + "java -jar seqware-distribution.jar " + Joiner.on(" ").join(runArgs) + "\n"
                + "Ini:\n" + Joiner.on("\n").withKeyValueSeparator("=").join(wr.getIniFile());
    }

    private boolean hasHandler(String workflowName, String workflowVersion) {
        for (Handler h : handlers) {
            if (h.isHandlerFor(workflowName, workflowVersion)) {
                return true;
            }
        }
        return false;
    }

    private <T extends Handler> T getHandler(String workflowName, String workflowVersion) {
        T handler = null;
        for (Handler h : handlers) {
            if (h.isHandlerFor(workflowName, workflowVersion)) {
                if (handler == null) {
                    handler = (T) h;
                } else {
                    throw new RuntimeException("Multiple handlers for workflow = [" + workflowName + "-" + workflowVersion + "]");
                }
            }
        }
        if (handler == null) {
            throw new RuntimeException("No handlers for workflow = [" + workflowName + "-" + workflowVersion + "]");
        }
        return handler;
    }

    private Path writeWorkflowRunIniPropertiesToFile(WorkflowRun wr) {
        try {
            Path iniFilePath = Files.createTempFile("bcl2fastq", ".ini");
            for (Map.Entry<String, String> e : wr.getIniFile().entrySet()) {
                String iniRecord = e.getKey() + "=" + e.getValue() + "\n";
                FileUtils.writeStringToFile(iniFilePath.toFile(), iniRecord, Charsets.UTF_8, true);
            }
            return iniFilePath;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private List<ProvenanceWithProvider<SampleProvenance>> applyBasesMask(List<ProvenanceWithProvider<SampleProvenance>> samples, BasesMask basesMask) throws DataMismatchException {
        List<ProvenanceWithProvider<SampleProvenance>> samplesWithCustomBarcodes = new ArrayList<>();
        for (ProvenanceWithProvider<SampleProvenance> pwp : samples) {
            SampleProvenance sp = pwp.getProvenance();
            Barcode newBarcode = BarcodeAndBasesMask.applyBasesMask(Barcode.fromString(sp.getIusTag()), basesMask);
            SampleProvenanceWithCustomBarcode sampleProvenanceWithCustomBarcode = new SampleProvenanceWithCustomBarcode(sp, newBarcode.toString());
            ProvenanceWithProvider<SampleProvenance> newPwp = new ProvenanceWithProvider(pwp.getProvider(), sampleProvenanceWithCustomBarcode);
            samplesWithCustomBarcodes.add(newPwp);
        }
        return samplesWithCustomBarcodes;
    }

    private void addInvalidLane(String message, String... args) {
        String error = MessageFormat.format(message, (Object[]) args);
        laneErrors.add(error);
    }

    private List<WorkflowRunV2> generateWorkflowRunsForLane(ProvenanceWithProvider<LaneProvenance> lp, Map<String, List<ProvenanceWithProvider<SampleProvenance>>> groupedSamples, Bcl2FastqHandler handler, boolean doDemultiplexing) throws InvalidLaneException {
        List<String> laneErrors = new ArrayList<>();
        List<WorkflowRunV2> workflowRuns = new ArrayList<>();
        for (Entry<String, List<ProvenanceWithProvider<SampleProvenance>>> e : groupedSamples.entrySet()) {
            String group = e.getKey();
            log.info("Generating workflow run for group = {}", group);

            List<ProvenanceWithProvider<SampleProvenance>> samplesForGroup = e.getValue();

            final List<String> barcodeErrors = new ArrayList<>();
            List<Barcode> workflowRunBarcodes = samplesForGroup.stream().map(ProvenanceWithProvider::getProvenance)
                    .map(SampleProvenance::getIusTag)
                    .map(barcodeString -> {
                        try {
                            return Barcode.fromString(barcodeString);
                        } catch (DataMismatchException ex) {
                            barcodeErrors.add(ex.toString());
                            return null;
                        }
                    })
                    .collect(Collectors.toList());
            if (!barcodeErrors.isEmpty()) {
                laneErrors.add(MessageFormat.format("Error while parsing barcodes for group = [{0}]:\n{1}", barcodeErrors.toString()));
                continue;
            }

            Bcl2FastqData data = new Bcl2FastqData(lp, samplesForGroup);
            data.setProperties(ImmutableMap.of("output_prefix", outputPath, "output_dir", outputFolder));
            data.setMetadataWriteback(getDoMetadataWriteback());
            data.setStudyToOutputPathConfig(studyToOutputPathConfig);

            BasesMask basesMask;
            if (!doDemultiplexing) {
                basesMask = null;
            } else {
                try {
                    if (overrideBasesMask != null) {
                        basesMask = BarcodeAndBasesMask.calculateBasesMask(workflowRunBarcodes, overrideBasesMask);
                    } else {
                        basesMask = BarcodeAndBasesMask.calculateBasesMask(workflowRunBarcodes);
                    }
                } catch (DataMismatchException ex) {
                    laneErrors.add(MessageFormat.format("Error while calculating bases mask for group = [{0}]:\n{1}", ex.toString()));
                    continue;
                }
            }
            data.setBasesMask(basesMask);

            WorkflowRunV2 wr = handler.getWorkflowRun(metadata, data, getDoCreateIusLimsKeys() && !getIsDryRunMode(), doDemultiplexing);

            if (wr.getErrors().isEmpty()) {
                workflowRuns.add(wr);
            } else {
                laneErrors.add(MessageFormat.format("Error while generating workflow run for group = [{0}], errors:\n{1}", group, Joiner.on("\n").join(wr.getErrors())));
            }
        }

        if (!laneErrors.isEmpty()) {
            throw new InvalidLaneException("Errors generating workflow runs for lane:\n" + Joiner.on("\n").join(laneErrors));
        }

        return workflowRuns;
    }

}
