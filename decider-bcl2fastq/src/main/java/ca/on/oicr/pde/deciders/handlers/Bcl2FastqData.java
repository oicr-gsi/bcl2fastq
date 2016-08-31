package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.IusWithProvenance;
import ca.on.oicr.pde.deciders.ProvenanceWithProvider;
import java.util.List;
import java.util.Map;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2FastqData {

    private Map<String, String> properties;
    private List<Integer> iusSwidsToLinkWorkflowRunTo;
    private List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> linkedSamples;
    private IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> linkedLane;
    private List<SampleProvenance> sps;
    private LaneProvenance lp;
    private Boolean metadataWriteback;

    public Bcl2FastqData() {

    }

    public Bcl2FastqData(Map<String, String> properties, List<Integer> iusSwidsToLinkWorkflowRunTo, List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> linkedSamples, IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> linkedLane, List<SampleProvenance> sps, LaneProvenance lp) {
        this.properties = properties;
        this.iusSwidsToLinkWorkflowRunTo = iusSwidsToLinkWorkflowRunTo;
        this.linkedSamples = linkedSamples;
        this.linkedLane = linkedLane;
        this.sps = sps;
        this.lp = lp;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public List<Integer> getIusSwidsToLinkWorkflowRunTo() {
        return iusSwidsToLinkWorkflowRunTo;
    }

    public void setIusSwidsToLinkWorkflowRunTo(List<Integer> iusSwidsToLinkWorkflowRunTo) {
        this.iusSwidsToLinkWorkflowRunTo = iusSwidsToLinkWorkflowRunTo;
    }

    public List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> getLinkedSamples() {
        return linkedSamples;
    }

    public void setLinkedSamples(List<IusWithProvenance<ProvenanceWithProvider<SampleProvenance>>> linkedSamples) {
        this.linkedSamples = linkedSamples;
    }

    public IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> getLinkedLane() {
        return linkedLane;
    }

    public void setLinkedLane(IusWithProvenance<ProvenanceWithProvider<LaneProvenance>> linkedLane) {
        this.linkedLane = linkedLane;
    }

    public List<SampleProvenance> getSps() {
        return sps;
    }

    public void setSps(List<SampleProvenance> sps) {
        this.sps = sps;
    }

    public LaneProvenance getLp() {
        return lp;
    }

    public void setLp(LaneProvenance lp) {
        this.lp = lp;
    }

    public Boolean getMetadataWriteback() {
        return metadataWriteback;
    }

    public void setMetadataWriteback(Boolean metadataWriteback) {
        this.metadataWriteback = metadataWriteback;
    }

}
