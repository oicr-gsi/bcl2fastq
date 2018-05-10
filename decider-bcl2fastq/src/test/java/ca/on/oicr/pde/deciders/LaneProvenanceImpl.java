package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import java.time.ZonedDateTime;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 *
 * @author mlaszloffy
 */
@Builder
@Value
public class LaneProvenanceImpl implements LaneProvenance {

    private String sequencerRunName;

    @Singular
    private SortedMap<String, SortedSet<String>> sequencerRunAttributes;
    private String sequencerRunPlatformModel;
    private String laneNumber;
    private SortedMap<String, SortedSet<String>> laneAttributes = new TreeMap<>();
    private Boolean skip;
    private ZonedDateTime createdDate;
    private String laneProvenanceId;
    private String provenanceId;
    private String version;
    private ZonedDateTime lastModified;
}
