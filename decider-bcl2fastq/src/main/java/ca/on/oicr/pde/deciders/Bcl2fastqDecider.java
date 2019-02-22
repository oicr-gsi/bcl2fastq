package ca.on.oicr.pde.deciders;

import ca.on.oicr.pde.deciders.data.ValidationResult;
import ca.on.oicr.pde.deciders.processor.LaneSplittingProcessor;
import ca.on.oicr.pde.deciders.data.*;
import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import ca.on.oicr.gsi.provenance.ExtendedProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.configuration.StudyToOutputPathConfig;
import ca.on.oicr.pde.deciders.handlers.Bcl2Fastq_2_7_1_Handler;
import ca.on.oicr.pde.deciders.handlers.Bcl2Fastq_2_9_1_Handler;
import ca.on.oicr.pde.deciders.handlers.Bcl2FastqHandler;
import ca.on.oicr.pde.deciders.handlers.Handler;
import ca.on.oicr.pde.deciders.exceptions.ConfigurationException;
import ca.on.oicr.pde.deciders.exceptions.InvalidBasesMaskException;
import ca.on.oicr.pde.deciders.exceptions.InvalidLaneException;
import ca.on.oicr.pde.deciders.handlers.Bcl2Fastq_2_9_2_Handler;
import ca.on.oicr.pde.deciders.utils.BarcodeComparison;
import ca.on.oicr.pde.deciders.utils.BarcodeAndBasesMask;
import ca.on.oicr.pde.deciders.utils.PineryClient;
import ca.on.oicr.pde.deciders.utils.RunScannerClient;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import net.sourceforge.seqware.common.metadata.Metadata;
import net.sourceforge.seqware.common.model.Workflow;
import net.sourceforge.seqware.pipeline.runner.PluginRunner;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author mlaszloffy
 */
public class Bcl2fastqDecider {

    private final Logger log = LogManager.getLogger(Bcl2fastqDecider.class);
    private final List<Bcl2FastqHandler> handlers = new ArrayList<>();

    private ExtendedProvenanceClient provenanceClient;
    private PineryClient pineryClient;
    private RunScannerClient runScannerClient;
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
    private Boolean processSkippedLanes = false;
    private Boolean provisionOutUndetermined = true;

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

    private BasesMask overrideRunBasesMask;

    private List<String> overrides;

    private final List<WorkflowRun> validWorkflowRuns = new ArrayList<>();
    private final List<WorkflowRun> scheduledWorkflowRuns = new ArrayList<>();
    private final List<String> laneErrors = new ArrayList<>();

    private Integer minAllowedEditDistance = 3;

    private LaneSplittingProcessor laneSplitting;
    private LaneSplittingProcessor.Mode runProcessingMode = LaneSplittingProcessor.Mode.AUTO;
    private Set<String> noLaneSplitWorkflowTypes = Sets.newHashSet("NovaSeqStandard");
    private Set<String> laneSplitWorkflowTypes = Sets.newHashSet("", "NovaSeqXp");

    public Bcl2fastqDecider() {
        //add workflow handlers
        handlers.add(new Bcl2Fastq_2_7_1_Handler()); //CASAVA 2.7.1 handler
        handlers.add(new Bcl2Fastq_2_9_1_Handler()); //CASAVA 2.9.1 handler
        handlers.add(new Bcl2Fastq_2_9_2_Handler()); //CASAVA 2.9.2 handler
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

    public void setDoLaneSplitting(boolean doLaneSplitting) {
        if (doLaneSplitting) {
            this.runProcessingMode = LaneSplittingProcessor.Mode.LANE_SPLITTING;
        } else {
            this.runProcessingMode = LaneSplittingProcessor.Mode.NO_LANE_SPLITTING;
        }
    }

    public Set<String> getNoLaneSplitWorkflowTypes() {
        return noLaneSplitWorkflowTypes;
    }

    public void setNoLaneSplitWorkflowTypes(Set<String> noLaneSplitWorkflowTypes) {
        this.noLaneSplitWorkflowTypes = noLaneSplitWorkflowTypes;
    }

    public Set<String> getLaneSplitWorkflowTypes() {
        return laneSplitWorkflowTypes;
    }

    public void setLaneSplitWorkflowTypes(Set<String> laneSplitWorkflowTypes) {
        this.laneSplitWorkflowTypes = laneSplitWorkflowTypes;
    }

    public Boolean getProcessSkippedLanes() {
        return processSkippedLanes;
    }

    public void setProcessSkippedLanes(Boolean processSkippedLanes) {
        this.processSkippedLanes = processSkippedLanes;
    }

    public Boolean getProvisionOutUndetermined() {
        return provisionOutUndetermined;
    }

    public void setProvisionOutUndetermined(Boolean provisionOutUndetermined) {
        this.provisionOutUndetermined = provisionOutUndetermined;
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

    public BasesMask getOverrideRunBasesMask() {
        return this.overrideRunBasesMask;
    }

    public void setOverrideRunBasesMask(BasesMask basesMask) {
        this.overrideRunBasesMask = basesMask;
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

    public void setPineryClient(PineryClient pineryClient) {
        this.pineryClient = pineryClient;
    }

    public void setRunScannerClient(RunScannerClient runScannerClient) {
        this.runScannerClient = runScannerClient;
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

        if (!disableRunCompleteCheck && pineryClient == null) {
            throw new ConfigurationException("Run complete check enabled but no pinery client has been configured");
        }

        if (runScannerClient == null) {
            laneSplitting = new LaneSplittingProcessor(this.runProcessingMode);
        } else {
            laneSplitting = new LaneSplittingProcessor(this.runProcessingMode, getNoLaneSplitWorkflowTypes(), getLaneSplitWorkflowTypes(), runScannerClient);
        }

        //reset workflow run collections
        validWorkflowRuns.clear();
        laneErrors.clear();
        scheduledWorkflowRuns.clear();

        // provenance data structures
        ListMultimap<String, ProvenanceWithProvider<LaneProvenance>> laneNameToLaneProvenance = ArrayListMultimap.create();
        Map<String, String> providerAndIdToLaneName = new HashMap<>();

        // get all lane provenance from providers specified in provenance settings
        Map<String, Collection<LaneProvenance>> laneProvenanceByProvider = provenanceClient.getLaneProvenanceByProvider(Collections.EMPTY_MAP);
        for (Map.Entry<String, Collection<LaneProvenance>> e : laneProvenanceByProvider.entrySet()) {
            String provider = e.getKey();
            for (LaneProvenance lp : e.getValue()) {
                String laneName = lp.getSequencerRunName() + "_lane_" + lp.getLaneNumber();
                providerAndIdToLaneName.put(provider + lp.getProvenanceId(), laneName);

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

                if (lp.getSkip()) {
                    if (getProcessSkippedLanes()) {
                        log.warn("Processing skipped lane = [{}]", laneName);
                    } else {
                        log.info("Lane = [{}] is skipped", laneName);
                        continue;
                    }
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
        Set<String> workflowSwidsToCheck = new HashSet<>();
        //TODO: seqware currently does not support retrieving FP with null workflow swids
        //workflowSwidsToCheck.add(null);
        workflowSwidsToCheck.add(workflow.getSwAccession().toString());
        workflowSwidsToCheck.addAll(getWorkflowAccessionsToCheck());
        analysisFilters.put(FileProvenanceFilter.workflow, workflowSwidsToCheck);

        //the set of all lanes that have been analyzed using the current workflow or a workflow in the set of "check workflows"
        Set<String> analyzedLanes = new HashSet<>();

        //calculate the set of lanes that do not have associated workflow information but are linked in seqware
        Set<String> blockedLanes = new HashSet<>();

        if (ignorePreviousLimsKeysMode) {
            // ignorePreviousLimsKeysMode enabled, no need to download any analysis as all known lanes are candidate lanes for analysis
        } else {
            Collection<? extends FileProvenance> fps = provenanceClient.getFileProvenance(analysisFilters);
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

        Predicate<SampleProvenance> is10x = (SampleProvenance sp) -> {
            if (sp.getSampleAttributes().containsKey("geo_prep_kit")
                    && sp.getSampleAttributes().get("geo_prep_kit").stream().anyMatch(s -> s.toLowerCase().contains("10x"))) {
                return true;
            }
            if (sp.getIusTag() != null && sp.getIusTag().startsWith("SI-")) {
                return true;
            }
            return false;
        };

        //lane validation and filtering before scheduling workflow run
        Set<String> invalidLanes = new HashSet<>();
        Set<String> filteredLanes = new HashSet<>();
        for (String laneName : candidateLanesToAnalyze) {
            List<String> laneValidationErrors = new ArrayList<>();

            //expect one lane provenance object per lane
            List<ProvenanceWithProvider<LaneProvenance>> lps = laneNameToLaneProvenance.get(laneName);
            if (lps.size() != 1) {
                reportInvalidLane(laneName, "Lane [%s] maps to multiple [%s] lane provenance records, one-to-one mapping is required.", laneName, Integer.toString(lps.size()));
                invalidLanes.add(laneName);
                continue;
            }
            LaneProvenance lp = Iterables.getOnlyElement(laneNameToLaneProvenance.get(laneName)).getProvenance();

            //expect one sample provenance provider per lane - or no samples in lane
            List<ProvenanceWithProvider<SampleProvenance>> sps = laneNameToSampleProvenance.get(laneName);

            if (sps.isEmpty()) {
                laneValidationErrors.add(String.format("Sample provenance count = [%s], expected 1 or more.", sps.size()));
            } else if (sps.stream().map(sp -> sp.getProvider()).distinct().collect(Collectors.toList()).size() > 1) {
                reportInvalidLane(laneName, "Lane [%s] has samples that map to multiple sample provenance providers, one-to-one mapping is required.", laneName);
                invalidLanes.add(laneName);
                continue;
            }

            //filter lane if all samples are 10X
            if (!sps.isEmpty() && sps.stream().map(sp -> sp.getProvenance()).allMatch(is10x)) {
                log.debug("Excluding lane = [{}] because all samples are 10X", laneName);
                filteredLanes.add(laneName);
                continue;
            }

            Optional<Boolean> doLaneSplitting = Optional.empty();
            try {
                doLaneSplitting = Optional.of(laneSplitting.getLaneSplitting(lp));
            } catch (InvalidLaneException ex) {
                reportInvalidLane(laneName, ex.getMessage());
                invalidLanes.add(laneName);
                continue;
            }
            if (doLaneSplitting.isPresent() && !doLaneSplitting.get()) {
                if (!"1".equals(lp.getLaneNumber())) {
                    //this is an no-lane-spliting (non-XP) run - only lane 1 should be processed
                    log.debug("Excluding lane = [{}] as a candidate lane due to workflowType == non-XP and lane number == {}", laneName, lp.getLaneNumber());
                    filteredLanes.add(laneName);
                    continue;
                }

                ValidationResult validNoLaneSplittingRun = laneSplitting.isValidNoLaneSplittingRun(lp.getSequencerRunName(), laneNameToSampleProvenance.values());
                if (!validNoLaneSplittingRun.isValid()) {
                    reportInvalidLane(laneName, validNoLaneSplittingRun.getReason());
                    invalidLanes.add(laneName);
                    continue;
                }
            }

            if (!disableRunCompleteCheck) {
                try {
                    String runStatus = pineryClient.getRunStatus(lp.getSequencerRunName()).orElse("");
                    if (!"Completed".equals(runStatus)) {
                        laneValidationErrors.add(String.format("Run state = [%s], expected [Completed])", runStatus));
                    }
                } catch (IOException ex) {
                    log.error("Failed to get run state from Pinery:", ex);
                    laneValidationErrors.add(String.format("Unable to get sequencer run state for run [%s]", lp.getSequencerRunName()));
                }
            }

            if (!laneValidationErrors.isEmpty()) {
                invalidLanes.add(laneName);
                log.warn("Lane = [{}] can not be processed due to the following reasons:\n"
                        + "{}\n"
                        + "Lane provenance: [{}]\n"
                        + "Sample provenance: [{}]",
                        laneName, Joiner.on("\n").join(laneValidationErrors), lp, Joiner.on(";").join(sps));
            }
        }

        //remove filtered lanes
        candidateLanesToAnalyze = Sets.difference(candidateLanesToAnalyze, filteredLanes);

        //remove invalid lanes from lanes to analyze set
        Set<String> lanesToAnalyze = Sets.difference(candidateLanesToAnalyze, invalidLanes);

        //collect required workflow run data - create IUS-LimsKey records in seqware that will be linked to the workflow run
        for (String laneName : lanesToAnalyze) {
            log.info("Processing lane [{}]", laneName);
            ProvenanceWithProvider<LaneProvenance> laneProvenanceAndProvider = Iterables.getOnlyElement(laneNameToLaneProvenance.get(laneName));
            LaneProvenance laneProvenance = laneProvenanceAndProvider.getProvenance();

            if (launchMax != null && validWorkflowRuns.size() >= launchMax) {
                log.info("Launch max [{}] reached.", launchMax);
                break;
            }

            //get samples in lane
            List<ProvenanceWithProvider<SampleProvenance>> samples = laneNameToSampleProvenance.get(laneName);

            //more than one sample in the lane, demultiplexing must be done for all workflow runs scheduled for this lane
            boolean doDemulitplexing = getIsDemultiplexSingleSampleMode();
            if (samples.size() > 1) {
                doDemulitplexing = true;
            }

            //filter out 10X samples
            samples = samples.stream().filter(s -> !is10x.test(s.getProvenance())).collect(Collectors.toList());

            //use overrideBasesMask or runBasesMask to calculate the sequenced barcodes
            Optional<BasesMask> runBasesMask = Optional.empty();
            if (overrideRunBasesMask != null) {
                runBasesMask = Optional.of(overrideRunBasesMask);
            } else if (laneProvenance.getSequencerRunAttributes().containsKey("run_bases_mask")) {
                SortedSet<String> runBasesMaskSet = laneProvenance.getSequencerRunAttributes().get("run_bases_mask");
                if (runBasesMaskSet.size() != 1) {
                    reportInvalidLane(laneName, "Expected one run_bases_mask, found: " + runBasesMaskSet.toString());
                    continue;
                }
                try {
                    runBasesMask = Optional.of(BasesMask.fromString(Iterables.getOnlyElement(runBasesMaskSet)));
                } catch (InvalidBasesMaskException ex) {
                    reportInvalidLane(laneName, ex.toString());
                    continue;
                }
            }
            if (runBasesMask.isPresent()) {
                try {
                    samples = applyBasesMask(samples, runBasesMask.get());
                } catch (DataMismatchException ex) {
                    reportInvalidLane(laneName, ex.toString());
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
                reportInvalidLane(laneName, "Unsupported barcode(s):\n" + unsupportedBarcodeStrings.toString());
                continue;
            }

            //check for barcode collisions
            List<BarcodeCollision> collisions = BarcodeComparison.getTruncatedHammingDistanceCollisions(laneBarcodes, minAllowedEditDistance);
            if (collisions.size() > 0) {
                reportInvalidLane(laneName, "Barcode collision(s):\n" + Joiner.on("\n").join(collisions));
                continue;
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
                validWorkflowRuns.addAll(generateWorkflowRunsForLane(laneProvenanceAndProvider, samplesGroupedByBarcodeLength, handler, runBasesMask.orElse(null), doDemulitplexing));
            } catch (InvalidLaneException ex) {
                reportInvalidLane(laneName, ex.toString());
            }
        }

        //list invalid lanes
        for (String invalidLane : this.laneErrors) {
            log.error(invalidLane);
            //log.warn("Invalid workflow run:\n" + debugWorkflowRun(wr));
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

    private void reportInvalidLane(String laneName, String error) {
        laneErrors.add(MessageFormat.format("Error while processing lane = [{0}]:\n{1}", laneName, error));
    }

    private void reportInvalidLane(String laneName, String message, String... args) {
        reportInvalidLane(laneName, MessageFormat.format(message, (Object[]) args));
    }

    private List<WorkflowRunV2> generateWorkflowRunsForLane(
            ProvenanceWithProvider<LaneProvenance> lp,
            Map<String, List<ProvenanceWithProvider<SampleProvenance>>> groupedSamples,
            Bcl2FastqHandler handler,
            BasesMask runBasesMask,
            boolean doDemultiplexing) throws InvalidLaneException {

        List<String> errors = new ArrayList<>();
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
                errors.add(MessageFormat.format("Error while parsing barcodes for group = [{0}]:\n{1}", barcodeErrors.toString()));
                continue;
            }

            Bcl2FastqData data = new Bcl2FastqData(lp, samplesForGroup);
            data.setProperties(ImmutableMap.of("output_prefix", outputPath, "output_dir", outputFolder));
            data.setMetadataWriteback(getDoMetadataWriteback());
            data.setStudyToOutputPathConfig(studyToOutputPathConfig);

            // support single end reads
            if (runBasesMask != null && runBasesMask.getReadTwoIncludeLength() == null) {
                data.setReadEnds("1");
            }

            BasesMask basesMask;
            if (!doDemultiplexing) {
                basesMask = null;
            } else {
                try {
                    if (runBasesMask != null) {
                        basesMask = BarcodeAndBasesMask.calculateBasesMask(workflowRunBarcodes, runBasesMask);
                    } else {
                        basesMask = BarcodeAndBasesMask.calculateBasesMask(workflowRunBarcodes);
                    }
                } catch (DataMismatchException ex) {
                    errors.add(MessageFormat.format("Error while calculating bases mask for group = [{0}]:\n{1}", ex.toString()));
                    continue;
                }
            }
            data.setBasesMask(basesMask);
            data.setDoLaneSplitting(laneSplitting.getLaneSplitting(lp.getProvenance()));
            data.setProvisionOutUndetermined(getProvisionOutUndetermined());

            WorkflowRunV2 wr = handler.getWorkflowRun(metadata, data, getDoCreateIusLimsKeys() && !getIsDryRunMode(), doDemultiplexing);

            if (wr.getErrors().isEmpty()) {
                workflowRuns.add(wr);
            } else {
                errors.add(MessageFormat.format("Error while generating workflow run for group = [{0}]:\n{1}", group, Joiner.on("\n").join(wr.getErrors())));
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidLaneException("Errors generating workflow runs for lane:\n" + Joiner.on("\n").join(errors));
        }

        return workflowRuns;
    }

}
