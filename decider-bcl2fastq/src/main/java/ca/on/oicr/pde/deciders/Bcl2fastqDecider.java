package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.IusLimsKey;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.LimsKey;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.handlers.*;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import joptsimple.OptionSpec;
import net.sourceforge.seqware.common.model.FileProvenanceParam;
import net.sourceforge.seqware.common.model.Workflow;
import net.sourceforge.seqware.common.module.ReturnValue;
import net.sourceforge.seqware.common.util.Log;
import net.sourceforge.seqware.pipeline.runner.PluginRunner;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

public class Bcl2fastqDecider extends OicrDecider {

    private Integer workflowSwid;
    private Integer launchMax;
    private boolean isIgnorePreviousAnalysisMode = false;
    private Map<FileProvenanceParam, List<String>> filters;
    private final OptionSpec<Integer> laneNumberOpt;
    private final OptionSpec<Boolean> ignorePreviousLimsKeysOpt;
    private final List<Bcl2FastqHandler> handlers = new ArrayList<>();

    public Bcl2fastqDecider() {
        super();

        //add workflow handlers
        handlers.add(new Bcl2Fastq1Handler()); //CASAVA 2.7 handler
        handlers.add(new Bcl2Fastq2Handler()); //CASAVA 2.8 handler

        laneNumberOpt = parser.accepts("lane-number", "The lane number that should be analyzed.").withRequiredArg().ofType(Integer.class);

        ignorePreviousLimsKeysOpt = parser.accepts("ignore-previous-lims-keys", "Flag to enable ignoring previous Lims Keys that were created.")
                .withOptionalArg().ofType(Boolean.class);
    }

    @Override
    public ReturnValue init() {
        ReturnValue rv = super.init();

        filters = parseOptions();

        Log.debug("INIT");

        if (options.has("launch-max")) {
            launchMax = Integer.parseInt(options.valueOf("launch-max").toString());
        }

        if (options.has("ignore-previous-runs") || options.has("force-run-all")) {
            isIgnorePreviousAnalysisMode = true;
        }

        workflowSwid = Integer.parseInt(getWorkflowAccession());

        return rv;
    }

    @Override
    public ReturnValue do_run() {
        ReturnValue rv = new ReturnValue(ReturnValue.SUCCESS);

        //get workflow handler
        Workflow workflow = metadata.getWorkflow(workflowSwid);
        String workflowName = workflow.getName();
        String workflowVersion = workflow.getVersion();
        if (!hasHandler(workflowName, workflowVersion)) {
            throw new RuntimeException("Workflow [" + workflowName + "-" + workflowVersion + "] is not supported");
        }
        Bcl2FastqHandler handler = getHandler(workflowName, workflowVersion);

        DateTime afterDateFilter = null;
        if (afterDate != null) {
            afterDateFilter = new DateTime(afterDate);
        }

        DateTime beforeDateFilter = null;
        if (beforeDate != null) {
            beforeDateFilter = new DateTime(beforeDate);
        }

        // provenance data structures
        SetMultimap<String, String> sequencerRunNameToLaneName = HashMultimap.create();
        ListMultimap<String, ProvenanceWithProvider<LaneProvenance>> laneNameToLaneProvenance = ArrayListMultimap.create();
        ListMultimap<String, ProvenanceWithProvider<SampleProvenance>> laneNameToSampleProvenance = ArrayListMultimap.create();
        Map<String, String> providerAndIdToLaneName = new HashMap<>();

        // get all lane provenance from providers specified in provenance settings
        Map<String, Collection<LaneProvenance>> laneProvenanceByProvider = provenanceClient.getLaneProvenanceByProvider(Collections.EMPTY_MAP);
        for (Entry<String, Collection<LaneProvenance>> e : laneProvenanceByProvider.entrySet()) {
            String provider = e.getKey();
            for (LaneProvenance lp : e.getValue()) {
                String laneName = lp.getSequencerRunName() + "_lane_" + lp.getLaneNumber();
                providerAndIdToLaneName.put(provider + lp.getLaneProvenanceId(), laneName);

                if (lp.getSkip()) {
                    continue;
                }

                if (afterDateFilter != null && lp.getCreatedDate().isBefore(afterDateFilter)) {
                    continue;
                }

                if (beforeDateFilter != null && lp.getCreatedDate().isAfter(beforeDateFilter)) {
                    continue;
                }

                sequencerRunNameToLaneName.put(lp.getSequencerRunName(), laneName);
                laneNameToLaneProvenance.put(laneName, new ProvenanceWithProvider<>(provider, lp));
            }
        }

        // get all sample provenance from providers specified in provenance settings
        Map<String, Collection<SampleProvenance>> sampleProvenanceByProvider = provenanceClient.getSampleProvenanceByProvider(Collections.EMPTY_MAP);
        for (Entry<String, Collection<SampleProvenance>> e : sampleProvenanceByProvider.entrySet()) {
            String provider = e.getKey();
            for (SampleProvenance sp : e.getValue()) {
                String laneName = sp.getSequencerRunName() + "_lane_" + sp.getLaneNumber();
                providerAndIdToLaneName.put(provider + sp.getSampleProvenanceId(), laneName);

                if (sp.getSkip()) {
                    continue;
                }

                if (afterDateFilter != null && sp.getCreatedDate().isBefore(afterDateFilter)) {
                    continue;
                }

                sequencerRunNameToLaneName.put(sp.getSequencerRunName(), laneName);
                laneNameToSampleProvenance.put(laneName, new ProvenanceWithProvider<>(provider, sp));
            }
        }

        // get all analysis provenance from providers specified in provenance settings
        ListMultimap<String, AnalysisProvenance> laneNameToAnalysisProvenance = ArrayListMultimap.create();
        ListMultimap<String, AnalysisProvenance> laneNameToPartialAnalysisProvenance = ArrayListMultimap.create();
        List<AnalysisProvenance> analysisProvenances = (List<AnalysisProvenance>) (List<?>) metadata.getAnalysisProvenance();
        for (AnalysisProvenance ap : analysisProvenances) {
            ListMultimap<String, AnalysisProvenance> map;
            if (ap.getWorkflowId() != null) { //analysis provenance for a worklow run
                map = laneNameToAnalysisProvenance;
                String currentAnalysisProvenanceWorkflowSwid = ap.getWorkflowId().toString();
                if (getWorkflowAccessionsToCheck().contains(currentAnalysisProvenanceWorkflowSwid)
                        || getWorkflowAccession().equals(currentAnalysisProvenanceWorkflowSwid)) {
                    //okay to include record
                } else {
                    //do not include this record, it is not of the current or related workflow type
                    continue;
                }
            } else { //analysis provenance for an ius
                //include this record, it may be an IUS that is used for skipping a lane or sample
                map = laneNameToPartialAnalysisProvenance;
            }

            for (IusLimsKey ilk : ap.getIusLimsKeys()) {
                LimsKey lk = ilk.getLimsKey();
                String provider = lk.getProvider();
                String id = lk.getId();

                String laneName = providerAndIdToLaneName.get(provider + id);
                if (laneName == null) {
                    Log.warn("Missing lane name for provider-id=[" + provider + "-" + id + "]");
                    continue;
                }

                map.put(laneName, ap);
            }
        }

        //calculate the set of all lanes that are known by the set of sample and lane provenance providers
        Set<String> allKnownLanes = Sets.union(laneNameToLaneProvenance.keySet(), laneNameToSampleProvenance.keySet());

        //calculate the set of all lanes that have been analyzed using the current workflow or a workflow in the set of "check workflows"
        Set<String> analyzedLanes = laneNameToAnalysisProvenance.keySet();

        //calculate the set of lanes that do not have associated workflow information but are linked in seqware
        Set<String> blockedLanes = laneNameToPartialAnalysisProvenance.keySet();

        //calculate the set of all lanes that are known and valid
        Set<String> allKnownValidLanes = Sets.difference(allKnownLanes, blockedLanes);

        //calculate the set of lanes that have not been processed
        Set<String> unprocessedLanes = Sets.difference(allKnownValidLanes, analyzedLanes);

        //calculate candidate set of lanes to process
        Set<String> candidateLaneNames = new HashSet<>();
        if (options.has("all") && filters.isEmpty()) {
            //no filters specified, so all unprocessed lanes are candidates
            candidateLaneNames = allKnownLanes;
        } else if (!options.has("all") && !filters.isEmpty()) {
            for (String currentLaneName : allKnownLanes) {
                if (filters.containsKey(FileProvenanceParam.sequencer_run)) {
                    for (String sequencerRunName : Sets.newHashSet(filters.get(FileProvenanceParam.sequencer_run))) {
                        if (options.has(laneNumberOpt)) {
                            if (currentLaneName.equals(sequencerRunName + "_lane_" + options.valueOf(laneNumberOpt))) {
                                candidateLaneNames.add(currentLaneName);
                            }
                        } else {
                            Set<String> laneNames = sequencerRunNameToLaneName.get(sequencerRunName);
                            if (laneNames.contains(currentLaneName)) {
                                candidateLaneNames.add(currentLaneName);
                            }
                        }
                    }
                }

                if (filters.containsKey(FileProvenanceParam.lane)) {
                    for (String laneName : Sets.newHashSet(filters.get(FileProvenanceParam.lane))) {
                        if (allKnownLanes.contains(laneName)) {
                            candidateLaneNames.add(currentLaneName);
                        } else {
                            //userSpecifiedLaneName is unknown
                        }
                    }
                }
            }
        } else {
            throw new RuntimeException("Unexpected combination of filter arguments");
        }

        //filter candidate lanes into lanes to analyze
        Set<String> lanesToAnalyze = new HashSet<>();
        if (options.has(ignorePreviousLimsKeysOpt)) {
            lanesToAnalyze.addAll(Sets.intersection(candidateLaneNames, allKnownLanes));
        } else if (isIgnorePreviousAnalysisMode) {
            lanesToAnalyze.addAll(Sets.intersection(candidateLaneNames, allKnownValidLanes));
        } else {
            lanesToAnalyze.addAll(Sets.intersection(candidateLaneNames, unprocessedLanes));
        }

        //check for prerequisite provenance objects (one lane provenance, one or more sample provenance)
        ListMultimap<String, String> invalidLanes = ArrayListMultimap.create();
        for (String laneName : lanesToAnalyze) {
            List<ProvenanceWithProvider<LaneProvenance>> lps = laneNameToLaneProvenance.get(laneName);
            if (lps.size() != 1) {
                invalidLanes.put(laneName, "Associated lane provenance count = " + lps.size() + ", expected 1");
            }

            List<ProvenanceWithProvider<SampleProvenance>> sps = laneNameToSampleProvenance.get(laneName);
            if (sps.isEmpty()) {
                invalidLanes.put(laneName, "Associated sample provenance count = " + sps.size() + ", expected 1 or more");
            }
        }

        //collect required workflow run data - create IUS-LimsKey records in seqware that will be linked to the workflow run
        List<WorkflowRun> workflowRuns = new ArrayList<>();
        for (String laneName : lanesToAnalyze) {

            if (launchMax != null && workflowRuns.size() >= launchMax) {
                break;
            }

            try {
                ProvenanceWithProvider<LaneProvenance> lane = Iterables.getOnlyElement(laneNameToLaneProvenance.get(laneName));
                IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> linkedLane = createIusToProvenanceLink(lane, getMetadataWriteback());
                LaneProvenance lp = lane.getProvenance();

                List<ProvenanceWithProvider<SampleProvenance>> samples = laneNameToSampleProvenance.get(laneName);
                List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> linkedSamples = new ArrayList<>();
                for (ProvenanceWithProvider<SampleProvenance> provenanceWithProvider : samples) {
                    linkedSamples.add(createIusToProvenanceLink(provenanceWithProvider, getMetadataWriteback()));
                }
                List<SampleProvenance> sps = new ArrayList<>();
                for (ProvenanceWithProvider<SampleProvenance> spWithProvider : samples) {
                    sps.add(spWithProvider.getProvenance());
                }

                List<Integer> iusSwidsToLinkWorkflowRunTo = new ArrayList<>();
                iusSwidsToLinkWorkflowRunTo.add(linkedLane.getIusSwid());
                for (IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> linkedSample : linkedSamples) {
                    iusSwidsToLinkWorkflowRunTo.add(linkedSample.getIusSwid());
                }

                Bcl2FastqData data = new Bcl2FastqData();
                data.setIusSwidsToLinkWorkflowRunTo(iusSwidsToLinkWorkflowRunTo);
                data.setLinkedLane(linkedLane);
                data.setLinkedSamples(linkedSamples);
                data.setLp(lp);
                data.setSps(sps);
                data.setProperties(getCommonIniProperties());
                data.setMetadataWriteback(getMetadataWriteback());

                WorkflowRun wr = handler.getWorkflowRun(data);
                workflowRuns.add(wr);
            } catch (DataMismatchException dme) {
                Log.error(laneName + " error: ", dme);
            }
        }

        //schedule workflow runs
        for (WorkflowRun wr : workflowRuns) {
            //write ini properties to file
            Path iniFilePath = writeWorkflowRunIniPropertiesToFile(wr);

            WorkflowSchedulerCommandBuilder cmdBuilder = new WorkflowSchedulerCommandBuilder(workflowSwid);
            cmdBuilder.setIniFile(iniFilePath);
            cmdBuilder.setMetadataWriteback(getMetadataWriteback());
            cmdBuilder.setIusSwidsToLinkWorkflowRunTo(wr.getIusSwidsToLinkWorkflowRunTo());
            cmdBuilder.setOverrideArgs(options.valuesOf(nonOptionSpec));
            List<String> runArgs = cmdBuilder.build();

            if (isDryRunMode()) {
                Log.stdout("Dry-run mode: not launching workflow");
                System.out.println(runArgs.toString());
            } else {
                Log.stdout("Scheduling workflow run.");
                PluginRunner pluginRunner = new PluginRunner();
                pluginRunner.setConfig(config);
                pluginRunner.run(runArgs.toArray(new String[runArgs.size()]));
            }
        }

        return rv;
    }

    @Override
    public ReturnValue do_summary() {
        return super.do_summary();
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

    private <T extends LimsProvenance> IusWithProvenance<ProvenanceWithProvider<T>> createIusToProvenanceLink(ProvenanceWithProvider<T> p, boolean doMetadataWriteback) {
        Integer iusSwid;
        if (doMetadataWriteback) {
            LimsKey lk = p.getLimsKey();
            Integer limsKeySwid = metadata.addLimsKey(p.getProvider(), lk.getId(), lk.getVersion(), lk.getLastModified());
            iusSwid = metadata.addIUS(limsKeySwid, false);
        } else {
            iusSwid = 0;
        }

        return new IusWithProvenance<>(iusSwid, p);
    }

    private Path writeWorkflowRunIniPropertiesToFile(WorkflowRun wr) {
        try {
            Path iniFilePath = Files.createTempFile("bcl2fastq", ".ini");
            for (Entry<String, String> e : wr.getIniFile().entrySet()) {
                String iniRecord = e.getKey() + "=" + e.getValue() + "\n";
                FileUtils.writeStringToFile(iniFilePath.toFile(), iniRecord, Charsets.UTF_8, true);
            }
            return iniFilePath;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String args[]) {
        List<String> params = new ArrayList<>();
        params.add("--plugin");
        params.add(Bcl2fastqDecider.class.getCanonicalName());
        params.add("--");
        params.add("--parent-wf-accession");//seqware plugin requires a parent-wf-accession
        params.add("0");
        params.addAll(Arrays.asList(args));
        System.out.println("Parameters: " + Arrays.deepToString(params.toArray()));
        net.sourceforge.seqware.pipeline.runner.PluginRunner.main(params.toArray(new String[params.size()]));
    }

}
