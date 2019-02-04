package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.LimsKey;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import ca.on.oicr.pde.deciders.data.IusWithProvenance;
import ca.on.oicr.pde.deciders.Lims;
import ca.on.oicr.pde.deciders.data.ProvenanceWithProvider;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import net.sourceforge.seqware.common.metadata.Metadata;

/**
 *
 * @author mlaszloffy
 */
public abstract class Bcl2FastqHandler implements Handler {

    public abstract void validate(Bcl2FastqData data, WorkflowRunV2 workflowRun);

    public abstract void modifyWorkflowRun(Bcl2FastqData data, WorkflowRunV2 workflowRun);

    public WorkflowRunV2 getWorkflowRun(Metadata metadata, Bcl2FastqData data, boolean createLimsKeys, boolean enableDemultiplexSingleSample) {
        WorkflowRunV2 wr = new WorkflowRunV2(null, null, data);
        wr.addProperty(data.getProperties());

        List<String> barcodes = new ArrayList<>();
        for (SampleProvenance sp : data.getSps()) {
            barcodes.add(sp.getIusTag());
        }

        if (enableDemultiplexSingleSample) {
            // do not do anything to barcode
        } else if (barcodes.size() == 1) {
            // do not do demultiplexing if there is only one sample in the lane https://jira.oicr.on.ca/browse/GR-261
            barcodes = Lists.newArrayList("NoIndex");
        } else {
            // multiple barcodes in lane
        }

        //check that there are no duplicate barcodes
        Set<String> uniqueBarcodes = Sets.newHashSet(barcodes);
        if (barcodes.size() != uniqueBarcodes.size()) {
            Set<String> barcodesTmp = new HashSet<>();
            Set<String> duplicates = new TreeSet<>();
            for (String barcode : barcodes) {
                if (!barcodesTmp.add(barcode)) {
                    duplicates.add(barcode);
                }
            }
            wr.addError("Duplicate barcodes detected = [" + Joiner.on(",").join(duplicates) + "]");
        }

        if (data.getBasesMask() != null) {
            wr.addProperty("use_bases_mask", data.getBasesMask().toString());
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

        if (data.getReadEnds() != null) {
            wr.addProperty("read_ends", data.getReadEnds());
        }

        //dry-run creating the "lane string" before actually creating IUS-LimsKeys
        try {
            IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> linkedLane = createIusToProvenanceLink(metadata, data.getLane(), false);
            List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> linkedSamples = new ArrayList<>();
            for (ProvenanceWithProvider<SampleProvenance> provenanceWithProvider : data.getSamples()) {
                IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> linkedSample = createIusToProvenanceLink(metadata, provenanceWithProvider, false);
                linkedSamples.add(linkedSample);
            }
            generateLinkingString(linkedLane, linkedSamples, enableDemultiplexSingleSample);
        } catch (DataMismatchException dme) {
            wr.addError(dme.toString());
        }

        //complete any additional workflow run validation
        validate(data, wr);

        //complete any additional workflow run modification
        modifyWorkflowRun(data, wr);

        //create IUS-LimsKeys and generate the workflow run "lanes" ini property
        if (wr.getErrors().isEmpty()) {
            try {
                List<Integer> iusSwidsToLinkWorkflowRunTo = new ArrayList<>();

                //create IUS-LimsKey for the lane
                IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> linkedLane = createIusToProvenanceLink(metadata, data.getLane(), createLimsKeys);
                iusSwidsToLinkWorkflowRunTo.add(linkedLane.getIusSwid());

                //create IUS-LimsKeys for the samples
                List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> linkedSamples = new ArrayList<>();
                for (ProvenanceWithProvider<SampleProvenance> provenanceWithProvider : data.getSamples()) {
                    IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> linkedSample = createIusToProvenanceLink(metadata, provenanceWithProvider, createLimsKeys);
                    linkedSamples.add(linkedSample);
                    iusSwidsToLinkWorkflowRunTo.add(linkedSample.getIusSwid());
                }

                wr.addProperty("lanes", generateLinkingString(linkedLane, linkedSamples, enableDemultiplexSingleSample));

                wr.setIusSwidsToLinkWorkflowRunTo(iusSwidsToLinkWorkflowRunTo);
            } catch (DataMismatchException dme) {
                wr.addError(dme.toString());
                wr.addProperty("lanes", "ERROR");
            }
        } else {
            wr.addProperty("lanes", "ERROR");
        }

        return wr;
    }

    private String generateLinkingString(IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> lane, List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> sps, boolean enableDemultiplexSingleSample) throws DataMismatchException {
        List<String> sampleStringEntries = new ArrayList<>();
        Collections.sort(sps, new Comparator<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>>() {
            @Override
            public int compare(IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> o1, IusWithProvenance<ProvenanceWithProvider<SampleProvenance>> o2) {
                return o1.getProvenanceWithProvider().getProvenance().getSampleName().compareTo(o2.getProvenanceWithProvider().getProvenance().getSampleName());
            }
        });

        if (sps.size() == 1) {
            if (enableDemultiplexSingleSample) {
                sampleStringEntries.add(generateSampleString(Iterables.getOnlyElement(sps), null));
            } else {
                // do not do demultiplexing if there is only one sample in the lane https://jira.oicr.on.ca/browse/GR-261
                sampleStringEntries.add(generateSampleString(Iterables.getOnlyElement(sps), "NoIndex"));
            }
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

    private <T extends LimsProvenance> IusWithProvenance<ProvenanceWithProvider<T>> createIusToProvenanceLink(Metadata metadata, ProvenanceWithProvider<T> p, boolean doMetadataWriteback) {
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
}
