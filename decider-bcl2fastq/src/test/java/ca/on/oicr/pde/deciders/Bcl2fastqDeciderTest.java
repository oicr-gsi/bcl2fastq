package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.LaneProvenanceProvider;
import ca.on.oicr.gsi.provenance.MultiThreadedDefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.ProvenanceClient;
import ca.on.oicr.gsi.provenance.SampleProvenanceProvider;
import ca.on.oicr.gsi.provenance.SeqwareMetadataAnalysisProvenanceProvider;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.client.MetadataBackedSeqwareClient;
import ca.on.oicr.pde.client.SeqwareClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import net.sourceforge.seqware.common.metadata.Metadata;
import net.sourceforge.seqware.common.metadata.MetadataInMemory;
import net.sourceforge.seqware.common.model.Workflow;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.powermock.reflect.Whitebox;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2fastqDeciderTest {

    private SampleProvenanceProvider spp;
    private LaneProvenanceProvider lpp;
    private ProvenanceClient provenanceClient;
    private Bcl2fastqDecider bcl2fastqDecider;
    private Workflow bcl2fastqWorkflow;
    private DateTime expectedDate = DateTime.parse("2016-01-01T00:00:00Z");

    @BeforeMethod
    public void setup() throws IOException {
        bcl2fastqDecider = new Bcl2fastqDecider();

        Map<String, String> config = new HashMap<>();
        config.put("SW_METADATA_METHOD", "inmemory");
        bcl2fastqDecider.setConfig(config);

        Metadata metadata = new MetadataInMemory();
        bcl2fastqDecider.setMetadata(metadata);

        String provider = "test";
        spp = Mockito.mock(SampleProvenanceProvider.class);
        lpp = Mockito.mock(LaneProvenanceProvider.class);

        MultiThreadedDefaultProvenanceClient client = new MultiThreadedDefaultProvenanceClient();
        provenanceClient = client;

        SeqwareMetadataAnalysisProvenanceProvider appX = new SeqwareMetadataAnalysisProvenanceProvider(metadata);
        client.registerAnalysisProvenanceProvider(provider, appX);
        client.registerLaneProvenanceProvider(provider, lpp);
        client.registerSampleProvenanceProvider(provider, spp);
        bcl2fastqDecider.setProvenanceClient(client);

        SeqwareClient seqwareClient = new MetadataBackedSeqwareClient(metadata, config);
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.7.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);

        File runDir = Files.createTempDir();
        runDir.deleteOnExit();
        File oicrCompleteFile = new File(runDir, "oicr_run_complete");
        oicrCompleteFile.createNewFile();
        oicrCompleteFile.deleteOnExit();
        LaneProvenance lp1 = LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .laneNumber("1")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of(runDir.getAbsolutePath()))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("h001"))
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("1_1")
                .version("1")
                .lastModified(expectedDate)
                .build();
        SampleProvenance sp1 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .skip(false)
                .provenanceId("1")
                .version("1")
                .lastModified(expectedDate)
                .build();

        LaneProvenance lp2 = LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0002")
                .laneNumber("1")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of(runDir.getAbsolutePath()))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("2_1")
                .version("1")
                .lastModified(expectedDate)
                .build();
        SampleProvenance sp2 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0002")
                .studyTitle("TEST_STUDY_2")
                .laneNumber("1")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .skip(false)
                .provenanceId("2")
                .version("1")
                .lastModified(expectedDate)
                .build();

        when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sp1, sp2));
        when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lp1, lp2));
    }

    @Test
    public void all() throws IOException {
        //run on all lanes
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        //all lanes have been scheduled - no new bcl2fastqWorkflow runs expected
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        //rerun everything
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(getFpsForCurrentWorkflow().size(), 8);

        //all should be blocked again
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8);
    }

    @Test
    public void beforeDateFilter() {
        bcl2fastqDecider.setBeforeDateFilter(expectedDate.minusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setBeforeDateFilter(expectedDate.plusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void afterDateFilter() {
        bcl2fastqDecider.setAfterDateFilter(expectedDate.plusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setAfterDateFilter(expectedDate.minusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void includeInstrumentFilter() {
        bcl2fastqDecider.setIncludeInstrumentNameFilter(ImmutableSet.of("does not exist"));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setIncludeInstrumentNameFilter(ImmutableSet.of("h001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }
    }

    @Test
    public void excludeInstrumentFilter() {
        bcl2fastqDecider.setExcludeInstrumentNameFilter(ImmutableSet.of("h001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0002"));
        }
    }

    @Test
    public void includeSequencerRunFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.sequencer_run, ImmutableSet.of("does not exist"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sequencer_run, ImmutableSet.of("RUN_0001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }
    }

    @Test
    public void excludeSequencerRunFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.sequencer_run, ImmutableSet.of("RUN_0001"));
        bcl2fastqDecider.setExcludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0002"));
        }
    }

    @Test
    public void includeLaneFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("does not exist"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }
    }

    @Test
    public void excludeLaneFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        bcl2fastqDecider.setExcludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0002"));
        }
    }

    @Test
    public void includeSequencerRunPlatformFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.sequencer_run_platform_model, ImmutableSet.of("does not exist"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sequencer_run_platform_model, ImmutableSet.of("HiSeq"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }
    }

    @Test
    public void excludeSequencerRunPlatformFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.sequencer_run_platform_model, ImmutableSet.of("HiSeq"));
        bcl2fastqDecider.setExcludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0002"));
        }
    }

    @Test
    public void includeStudyFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.study, ImmutableSet.of("does not exist"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.study, ImmutableSet.of("TEST_STUDY_1"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }
    }

    @Test
    public void excludeStudyFilter() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.study, ImmutableSet.of("TEST_STUDY_1"));
        bcl2fastqDecider.setExcludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0002"));
        }
    }

    @Test
    public void dryRunOverrides() {
        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.study, ImmutableSet.of("TEST_STUDY_1"));
        bcl2fastqDecider.setIncludeFilters(filters);

        bcl2fastqDecider.setIsDryRunMode(true);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        bcl2fastqDecider.setIgnorePreviousLimsKeysMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);

        Collection<FileProvenance> fps = getFpsForCurrentWorkflow();
        assertEquals(fps.size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }

        //clear static "STORE" table in MetadataInMemory after this test - it creates blocking Ius-LimsKeys
        Whitebox.<Table>getInternalState(MetadataInMemory.class, "STORE").clear();
    }

    private Collection<FileProvenance> getFpsForCurrentWorkflow() {
        return provenanceClient.getFileProvenance(
                ImmutableMap.<FileProvenanceFilter, Set<String>>of(FileProvenanceFilter.workflow, ImmutableSet.of(bcl2fastqWorkflow.getSwAccession().toString())));
    }

    @Builder
    @Value
    private static class LaneProvenanceImpl implements LaneProvenance {

        private String sequencerRunName;

        @Singular
        private SortedMap<String, SortedSet<String>> sequencerRunAttributes;
        private String sequencerRunPlatformModel;
        private String laneNumber;
        private SortedMap<String, SortedSet<String>> laneAttributes = new TreeMap<>();
        private Boolean skip;
        private DateTime createdDate;
        private String laneProvenanceId;
        private String provenanceId;
        private String version;
        private DateTime lastModified;
    }

    @Builder
    @Value
    private static class SampleProvenanceImpl implements SampleProvenance {

        private String studyTitle;
        private SortedMap<String, SortedSet<String>> studyAttributes = new TreeMap<>();
        private String rootSampleName;
        private String parentSampleName;
        private String sampleName;
        private SortedMap<String, SortedSet<String>> sampleAttributes = new TreeMap<>();
        private String sequencerRunName;
        private SortedMap<String, SortedSet<String>> sequencerRunAttributes = new TreeMap<>();
        private String sequencerRunPlatformModel;
        private String laneNumber;
        private SortedMap<String, SortedSet<String>> laneAttributes = new TreeMap<>();
        private String iusTag;
        private Boolean skip;
        private DateTime createdDate;
        private String sampleProvenanceId;
        private String provenanceId;
        private String version;
        private DateTime lastModified;
    }
}
