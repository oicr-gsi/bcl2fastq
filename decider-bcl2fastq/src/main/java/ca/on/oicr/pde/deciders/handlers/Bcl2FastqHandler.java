package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.DataMismatchException;
import ca.on.oicr.pde.deciders.IusWithProvenance;
import ca.on.oicr.pde.deciders.Lims;
import ca.on.oicr.pde.deciders.ProvenanceWithProvider;
import ca.on.oicr.pde.deciders.WorkflowRunV2;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 *
 * @author mlaszloffy
 */
public abstract class Bcl2FastqHandler implements Handler {

    public abstract WorkflowRunV2 modifyWorkflowRun(Bcl2FastqData data, WorkflowRunV2 workflowRun);

    public WorkflowRunV2 getWorkflowRun(Bcl2FastqData data) {
        WorkflowRunV2 wr = new WorkflowRunV2(null, null);
        wr.addProperty(data.getProperties());

        wr.setIusSwidsToLinkWorkflowRunTo(data.getIusSwidsToLinkWorkflowRunTo());

        try {
            wr.addProperty("lanes", generateLinkingString(data.getLinkedLane(), data.getLinkedSamples()));
        } catch (DataMismatchException dme) {
            wr.addError(dme.toString());
            wr.addProperty("lanes", "ERROR");
        }

        List<String> barcodes = new ArrayList<>();
        for (SampleProvenance sp : data.getSps()) {
            barcodes.add(sp.getIusTag());
        }
        if (barcodes.size() == 1) {
            // do not do demultiplexing if there is only one sample in the lane https://jira.oicr.on.ca/browse/GR-261
            barcodes = Lists.newArrayList("NoIndex");
        }

        String basesMask;
        try {
            basesMask = calculateBasesMask(barcodes);
        } catch (DataMismatchException dme) {
            basesMask = "ERROR";
            wr.addError(dme.getMessage());
        }
        if (basesMask != null) {
            wr.addProperty("use_bases_mask", basesMask);
        }

        String runDir = null;
        SortedSet<String> runDirs = data.getLp().getSequencerRunAttributes().get("run_dir");
        if (runDirs == null || runDirs.size() != 1) {
            runDir = "ERROR";
            wr.addError("Run dir is missing");
        } else {
            runDir = Iterables.getOnlyElement(runDirs);
            if (!runDir.endsWith("/")) {
                runDir = runDir + "/";
            }
        }
        wr.addProperty("intensity_folder", runDir + "Data/Intensities/");
        wr.addProperty("called_bases", runDir + "Data/Intensities/BaseCalls/");

        if (data.getMetadataWriteback()) {
            wr.addProperty("metadata", "metadata");
        } else {
            wr.addProperty("metadata", "no-metadata");
        }

        wr.addProperty("flowcell", data.getLp().getSequencerRunName());

        if (data.getStudyToOutputPathConfig() != null) {
            Set<String> outputPaths = new HashSet<>();
            for (SampleProvenance sp : data.getSps()) {
                //Get the output path from the study to output path csv
                String outputPath = data.getStudyToOutputPathConfig().getOutputPathForStudy(sp.getStudyTitle());
                outputPaths.add(outputPath);
            }
            String outputPrefix;
            if (outputPaths.size() == 1) {
                outputPrefix = Iterables.getOnlyElement(outputPaths);
            } else {
                outputPrefix = "ERROR";
                wr.addError("[" + outputPaths.size() + "] output paths found for workflow run - expected one.");
            }
            wr.addProperty("output_prefix", outputPrefix);
        }

        wr = modifyWorkflowRun(data, wr);

        return wr;
    }

    public static String calculateBasesMask(List<String> barcodes) throws DataMismatchException {

        if (barcodes == null || barcodes.isEmpty()) {
            return null;
        }

        Set<Boolean> isNoIndexSet = new HashSet<>();
        for (String barcode : barcodes) {
            isNoIndexSet.add(barcode == null || barcode.isEmpty() || "NoIndex".equals(barcode));
        }

        Set<Boolean> isDualBarcodeSet = new HashSet<>();
        for (String barcode : barcodes) {
            isDualBarcodeSet.add(barcode != null && barcode.contains("-"));
        }

        Set<Integer> barcodeLengths = new HashSet<>();
        for (String barcode : barcodes) {
            if (barcode == null || barcode.isEmpty() || "NoIndex".equals(barcode)) {
                barcodeLengths.add(0);
            } else {
                barcodeLengths.add(barcode.length());
            }
        }

        if (isNoIndexSet.size() != 1) {
            throw new DataMismatchException("Combination of NoIndex and barcoded samples");
        }

        if (isDualBarcodeSet.size() != 1) {
            throw new DataMismatchException("Combination of dual-barcode and non-dual-barcode");
        }

        if (barcodeLengths.size() != 1) {
            throw new DataMismatchException("Combination of barcodes with different lengths");
        }

        boolean isNoIndex = Iterables.getOnlyElement(isNoIndexSet);
        boolean isDualBarcode = Iterables.getOnlyElement(isDualBarcodeSet);
        int barcodeLength = Iterables.getOnlyElement(barcodeLengths);

        if (isNoIndex) {
            return null;
        } else if (isDualBarcode) {
            return "y*,I*,I*,y*";
        } else if (barcodeLength > 0) {
            return "y*,I" + barcodeLength + "n*,y*";
        } else {
            throw new DataMismatchException("Unable to calculate bases mask");
        }
    }

    private String generateLinkingString(IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> lane, List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> sps) throws DataMismatchException {
        List<String> sampleStringEntries = new ArrayList<>();
        Collections.sort(sps, new Comparator<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>>() {
            @Override
            public int compare(IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> o1, IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> o2) {
                return o1.getProvenanceWithProvider().getProvenance().getSampleName().compareTo(o2.getProvenanceWithProvider().getProvenance().getSampleName());
            }
        });
        if (sps.size() == 1) {
            // do not do demultiplexing if there is only one sample in the lane https://jira.oicr.on.ca/browse/GR-261
            sampleStringEntries.add(generateSampleString(Iterables.getOnlyElement(sps), "NoIndex"));
        } else {
            for (IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> s : sps) {
                sampleStringEntries.add(generateSampleString(s, null));
            }
        }
        String sampleString = Joiner.on("+").join(sampleStringEntries);

        return generateLaneString(lane) + ":" + sampleString;
    }

    private String generateLaneString(String laneNumber, String laneIusSwid) {
        return laneNumber + "," + laneIusSwid;
    }

    private String generateLaneString(IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> lane) {
        LaneProvenance lp = lane.getProvenanceWithProvider().getProvenance();
        return generateLaneString(lp.getLaneNumber(), lane.getIusSwid().toString());
    }

    private String generateSampleString(String iusTag, String iusSwid, String sampleName, String groupId) {
        String sampleString = iusTag + "," + iusSwid + "," + sampleName;
        if (groupId != null && !groupId.isEmpty()) {
            sampleString = sampleString + "," + groupId;
        }
        return sampleString;
    }

    private String generateSampleString(IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> sample, String barcodeOverride) throws DataMismatchException {
        SampleProvenance sp = sample.getProvenanceWithProvider().getProvenance();

        String barcode;
        if (barcodeOverride != null) {
            barcode = barcodeOverride;
        } else {
            barcode = sp.getIusTag();
        }

        String groupId = null;
        Collection<String> groupIds = sp.getSampleAttributes().get(Lims.GROUP_ID.getAttributeTitle());
        if (groupIds != null && !groupIds.isEmpty()) {
            if (groupIds.size() != 1) {
                throw new DataMismatchException("[" + groupIds.size() + "] group ids were detected for "
                        + "sample = [" + sp.getSampleName() + "], sample provenance id = [" + sp.getSampleProvenanceId() + "]");
            }
            groupId = Iterables.getOnlyElement(groupIds);
        }

        return generateSampleString(barcode, sample.getIusSwid().toString(), sp.getSampleName(), groupId);
    }

}
