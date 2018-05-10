package ca.on.oicr.pde.deciders.data;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.deciders.configuration.StudyToOutputPathConfig;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2FastqData {

    private Map<String, String> properties;
    private final ProvenanceWithProvider<LaneProvenance> lane;
    private final List<ProvenanceWithProvider<SampleProvenance>> samples;
    private Boolean metadataWriteback;
    private StudyToOutputPathConfig studyToOutputPathConfig;

    public Bcl2FastqData(ProvenanceWithProvider<LaneProvenance> lane, List<ProvenanceWithProvider<SampleProvenance>> samples) {
        this.lane = lane;
        this.samples = samples;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public ProvenanceWithProvider<LaneProvenance> getLane() {
        return lane;
    }

    public List<ProvenanceWithProvider<SampleProvenance>> getSamples() {
        return samples;
    }

    public List<SampleProvenance> getSps() {
        return Lists.transform(samples, new Function<ProvenanceWithProvider<SampleProvenance>, SampleProvenance>() {
            @Override
            public SampleProvenance apply(ProvenanceWithProvider<SampleProvenance> input) {
                return input.getProvenance();
            }
        });
    }

    public LaneProvenance getLp() {
        return lane.getProvenance();
    }

    public Boolean getMetadataWriteback() {
        return metadataWriteback;
    }

    public void setMetadataWriteback(Boolean metadataWriteback) {
        this.metadataWriteback = metadataWriteback;
    }

    public StudyToOutputPathConfig getStudyToOutputPathConfig() {
        return studyToOutputPathConfig;
    }

    public void setStudyToOutputPathConfig(StudyToOutputPathConfig studyToOutputPathConfig) {
        this.studyToOutputPathConfig = studyToOutputPathConfig;
    }

}
