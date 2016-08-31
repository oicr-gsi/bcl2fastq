package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.DataMismatchException;
import ca.on.oicr.pde.deciders.IusWithProvenance;
import ca.on.oicr.pde.deciders.ProvenanceWithProvider;
import ca.on.oicr.pde.deciders.WorkflowRun;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 *
 * @author mlaszloffy
 */
public abstract class Bcl2FastqHandler implements Handler {

    public abstract WorkflowRun modifyWorkflowRun(Bcl2FastqData data, WorkflowRun workflowRun) throws DataMismatchException;

    public WorkflowRun getWorkflowRun(Bcl2FastqData data) throws DataMismatchException {
        WorkflowRun wr = new WorkflowRun(null, null);
        wr.addProperty(data.getProperties());

        wr.setIusSwidsToLinkWorkflowRunTo(data.getIusSwidsToLinkWorkflowRunTo());

        String laneString;
        if (data.getLinkedSamples().size() == 1) {
            // do not do demultiplexing if there is only one sample in the lane https://jira.oicr.on.ca/browse/GR-261
            laneString = generateLinkingStringAndOverrideBarcode(data.getLinkedLane(), Iterables.getOnlyElement(data.getLinkedSamples()), "NoIndex");
        } else {
            laneString = generateLinkingString(data.getLinkedLane(), data.getLinkedSamples());
        }
        wr.addProperty("lanes", laneString);

        List<String> barcodes = new ArrayList<>();
        for (SampleProvenance sp : data.getSps()) {
            barcodes.add(sp.getIusTag());
        }
        if (barcodes.size() == 1) {
            // do not do demultiplexing if there is only one sample in the lane https://jira.oicr.on.ca/browse/GR-261
            barcodes = Lists.newArrayList("NoIndex");
        }
        String basesMask = calculateBasesMask(barcodes);
        if (basesMask != null) {
            wr.addProperty("use_bases_mask", basesMask);
        }

        String runDir = null;
        SortedSet<String> runDirs = data.getLp().getSequencerRunAttributes().get("run_dir");
        if (runDirs == null || runDirs.size() != 1) {
            throw new DataMismatchException("Run dir is missing");
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

        wr = modifyWorkflowRun(data, wr);

        return wr;
    }

    public static String calculateBasesMask(List<String> barcodes) throws DataMismatchException {

        if (barcodes == null || barcodes.isEmpty()) {
            return null;
        }

        Set<Boolean> isNoIndexSet = new HashSet<>();
        for (String barcode : barcodes) {
            isNoIndexSet.add(barcode.equals("NoIndex"));
        }

        Set<Boolean> isDualBarcodeSet = new HashSet<>();
        for (String barcode : barcodes) {
            isDualBarcodeSet.add(barcode.contains("-"));
        }

        Set<Integer> barcodeLengths = new HashSet<>();
        for (String barcode : barcodes) {
            barcodeLengths.add(barcode.length());
        }

        if (isNoIndexSet.size() != 1) {
            throw new DataMismatchException("Combination of NoIndex and barcodes");
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
            throw new RuntimeException("");
        }
    }

    private String generateLinkingString(IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> lane, List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> samples) {
        Integer laneProvenanceIusSwid = lane.getIusSwid();
        LaneProvenance lp = lane.getProvenanceWithProvider().getProvenance();
        String laneString = lp.getLaneNumber() + "," + laneProvenanceIusSwid;

        List<String> es = new ArrayList<>();
        for (IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> s : samples) {
            Integer sampleProvenanceIusSwid = s.getIusSwid();
            SampleProvenance sp = s.getProvenanceWithProvider().getProvenance();
            es.add(sp.getIusTag() + "," + sampleProvenanceIusSwid + "," + sp.getSampleName());
        }
        String sampleString = Joiner.on("+").join(es);

        return laneString + ":" + sampleString;
    }

    private String generateLinkingStringAndOverrideBarcode(IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> lane, IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> sample, String barcode) {
        Integer laneProvenanceIusSwid = lane.getIusSwid();
        LaneProvenance lp = lane.getProvenanceWithProvider().getProvenance();
        String laneString = lp.getLaneNumber() + "," + laneProvenanceIusSwid;

        Integer sampleProvenanceIusSwid = sample.getIusSwid();
        SampleProvenance sp = sample.getProvenanceWithProvider().getProvenance();
        String sampleString = barcode + "," + sampleProvenanceIusSwid + "," + sp.getSampleName();

        return laneString + ":" + sampleString;
    }

}
