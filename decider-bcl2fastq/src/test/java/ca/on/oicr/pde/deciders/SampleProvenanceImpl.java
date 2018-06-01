package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import java.time.ZonedDateTime;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import lombok.Builder;
import lombok.Value;

/**
 *
 * @author mlaszloffy
 */
@Builder
@Value
public class SampleProvenanceImpl implements SampleProvenance {

    private String studyTitle;
    private SortedMap<String, SortedSet<String>> studyAttributes = new TreeMap<>();
    private String rootSampleName;
    private String parentSampleName;
    private String sampleName;
    private SortedMap<String, SortedSet<String>> sampleAttributes;
    private String sequencerRunName;
    private SortedMap<String, SortedSet<String>> sequencerRunAttributes = new TreeMap<>();
    private String sequencerRunPlatformModel;
    private String laneNumber;
    private SortedMap<String, SortedSet<String>> laneAttributes = new TreeMap<>();
    private String iusTag;
    private Boolean skip;
    private ZonedDateTime createdDate;
    private String sampleProvenanceId;
    private String provenanceId;
    private String version;
    private ZonedDateTime lastModified;
}
