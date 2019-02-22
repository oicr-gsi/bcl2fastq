package ca.on.oicr.pde.deciders.processor;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.Bcl2fastqDecider;
import ca.on.oicr.pde.deciders.data.ValidationResult;
import ca.on.oicr.pde.deciders.data.ProvenanceWithProvider;
import ca.on.oicr.pde.deciders.exceptions.InvalidLaneException;
import ca.on.oicr.pde.deciders.utils.RunScannerClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LaneSplittingProcessor {

    public enum Mode {
        AUTO,
        LANE_SPLITTING,
        NO_LANE_SPLITTING;
    }

    private Mode runProcessingMode;
    private Set<String> noLaneSplitWorkflowTypes;
    private Set<String> laneSplitWorkflowTypes;
    private LoadingCache<String, String> workflowTypesByRunName;

    public LaneSplittingProcessor(Mode runProcessingMode) {
        if (!(runProcessingMode == Mode.LANE_SPLITTING
                || runProcessingMode == Mode.NO_LANE_SPLITTING)) {
            throw new IllegalArgumentException("Lane splitting mode needs to be specified if Run Scanner is not specified.");
        }
        this.runProcessingMode = runProcessingMode;
    }

    public LaneSplittingProcessor(Mode runProcessingMode,
            Set<String> noLaneSplitWorkflowTypes,
            Set<String> laneSplitWorkflowTypes,
            RunScannerClient runScannerClient) {

        this.runProcessingMode = runProcessingMode;
        this.noLaneSplitWorkflowTypes = new HashSet<>(noLaneSplitWorkflowTypes);
        this.laneSplitWorkflowTypes = new HashSet<>(laneSplitWorkflowTypes);

        // workflowType sanity check
        if (!Sets.intersection(noLaneSplitWorkflowTypes, laneSplitWorkflowTypes).isEmpty()) {
            throw new IllegalArgumentException("Duplicate entry detected in laneSplitWorkflowTypes vs. noLaneSplitWorkflowTypes");
        }

        //workflowType source is currently (2019-02-01) only available via Run Scanner
        workflowTypesByRunName = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String runName) throws IOException {
                        return runScannerClient.getRunWorkflowType(runName).orElse("");
                    }
                });
    }

    public boolean getLaneSplitting(LaneProvenance lp) throws InvalidLaneException {
        if (runProcessingMode == Mode.AUTO) {
            String workflowType = null;  //TODO: get value from lane provenance attribute when available
            try {
                workflowType = workflowTypesByRunName.get(lp.getSequencerRunName());
            } catch (ExecutionException ee) {
                throw new InvalidLaneException(String.format("Unable to get workflowType:\n[%s]", ee));
            }
            if (laneSplitWorkflowTypes.contains(workflowType) && !noLaneSplitWorkflowTypes.contains(workflowType)) {
                return true;
            } else if (!laneSplitWorkflowTypes.contains(workflowType) && noLaneSplitWorkflowTypes.contains(workflowType)) {
                return false;
            } else {
                throw new InvalidLaneException(String.format("workflowType [%s] is unsupported", workflowType));
            }
        } else if (runProcessingMode == Mode.LANE_SPLITTING) {
            return true;
        } else if (runProcessingMode == Mode.NO_LANE_SPLITTING) {
            return false;
        } else {
            throw new RuntimeException(String.format("Unsupported RunProcessingMode [%s]", runProcessingMode));
        }
    }

    public ValidationResult isValidNoLaneSplittingRun(String runName, Collection<ProvenanceWithProvider<SampleProvenance>> sps) {

        List<SampleProvenance> runSamples = sps.stream()
                .map(ss -> ss.getProvenance())
                .filter(s -> runName.equals(s.getSequencerRunName()))
                .collect(Collectors.toList());

        // check if no samples have been assigned to run (which is a valid state) - warnings for empty runs are handled upstream
        if (runSamples.isEmpty()) {
            return ValidationResult.valid();
        }

        Map<String, List<String>> runSamplesGroupByLane = runSamples.stream().collect(
                Collectors.groupingBy(s -> s.getLaneNumber(),
                        //map sample provenance object to string representation for the following verification step
                        Collectors.mapping(s -> s.getSampleName() + "-" + s.getIusTag(),
                                Collectors.toList()))
        );

        if (runSamplesGroupByLane.values().stream()
                .filter(l -> !l.isEmpty())
                .map(l -> l.stream().sorted().collect(Collectors.toList()))
                .distinct()
                .count() != 1) {
            return ValidationResult.invalid("Operating in no-lane-splitting mode and different samples in lanes detected, run = [{0}]:\n{1}", runName, runSamplesGroupByLane.toString());
        } else if (runSamplesGroupByLane.get("1") == null || runSamplesGroupByLane.get("1").isEmpty()) {
            return ValidationResult.invalid("Operating in no-lane-splitting mode and no samples detected in lane 1, run = [{0}]:\n{1}", runName, runSamplesGroupByLane.toString());
        } else {
            return ValidationResult.valid();
        }
    }

}
