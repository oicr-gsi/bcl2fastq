package ca.on.oicr.pde.deciders;

import ca.on.oicr.pde.deciders.data.WorkflowRunV2;
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
import ca.on.oicr.pde.deciders.data.BasesMask;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import net.sourceforge.seqware.common.metadata.Metadata;
import net.sourceforge.seqware.common.metadata.MetadataInMemory;
import net.sourceforge.seqware.common.model.Workflow;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.powermock.reflect.Whitebox;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.AfterMethod;
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
    private ZonedDateTime expectedDate = ZonedDateTime.parse("2016-01-01T00:00:00Z");

    private SortedMap<String, SortedSet<String>> sp1Attrs = new TreeMap<>();
    private SortedMap<String, SortedSet<String>> sp2Attrs = new TreeMap<>();

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
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA")
                .sampleAttributes(sp1Attrs)
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
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA")
                .sampleAttributes(sp2Attrs)
                .skip(false)
                .provenanceId("2")
                .version("1")
                .lastModified(expectedDate)
                .build();

        when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sp1, sp2));
        when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lp1, lp2));
    }

    @AfterMethod
    public void destroySeqware() {
        Whitebox.<Table>getInternalState(MetadataInMemory.class, "STORE").clear();
    }

    @Test
    public void all() throws IOException {
        //run on all lanes
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        //all lanes have been scheduled - no new bcl2fastqWorkflow runs expected
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        //rerun everything
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8);

        //all should be blocked again
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8);
    }

    @Test
    public void beforeDateFilter() {
        bcl2fastqDecider.setBeforeDateFilter(expectedDate.minusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setBeforeDateFilter(expectedDate.plusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void afterDateFilter() {
        bcl2fastqDecider.setAfterDateFilter(expectedDate.plusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setAfterDateFilter(expectedDate.minusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void includeInstrumentFilter() {
        bcl2fastqDecider.setIncludeInstrumentNameFilter(ImmutableSet.of("does not exist"));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setIncludeInstrumentNameFilter(ImmutableSet.of("h001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }
    }

    @Test
    public void excludeInstrumentFilter() {
        bcl2fastqDecider.setExcludeInstrumentNameFilter(ImmutableSet.of("h001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sequencer_run, ImmutableSet.of("RUN_0001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sequencer_run_platform_model, ImmutableSet.of("HiSeq"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.study, ImmutableSet.of("TEST_STUDY_1"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
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
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        bcl2fastqDecider.setIgnorePreviousLimsKeysMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);

        Collection<FileProvenance> fps = getFpsForCurrentWorkflow();
        assertEquals(fps.size(), 2);
        for (FileProvenance fp : getFpsForCurrentWorkflow()) {
            assertEquals(fp.getSequencerRunNames(), ImmutableSet.of("RUN_0001"));
        }

        //clear static "STORE" table in MetadataInMemory after this test - it creates blocking Ius-LimsKeys
        Whitebox.<Table>getInternalState(MetadataInMemory.class, "STORE").clear();
    }

    @Test
    public void multipleGroupIdTest() {
        sp2Attrs.clear();
        sp2Attrs.put(Lims.GROUP_ID.getAttributeTitle(), ImmutableSortedSet.of("group1", "group2"));

        //sp2 has two group ids, can not be run
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);

        //correct the group ids
        sp2Attrs.clear();
        sp2Attrs.put(Lims.GROUP_ID.getAttributeTitle(), ImmutableSortedSet.of("group2"));

        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void duplicateBarcodeTest() {
        SampleProvenanceImpl.SampleProvenanceImplBuilder sp = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0002")
                .studyTitle("TEST_STUDY_2")
                .laneNumber("1")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0003")
                .sampleName("TEST_0003_001")
                .iusTag("AAAAAAAA")
                .sampleAttributes(sp2Attrs)
                .skip(false)
                .provenanceId("9")
                .version("1")
                .lastModified(expectedDate);

        List<SampleProvenance> currentList = Lists.newArrayList(spp.getSampleProvenance());
        when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));

        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);

        //correct the barcode and schedule
        sp.iusTag("TTTTTTTT");
        when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 5);
    }

    @Test
    public void barcodeLengthMismatchTest1() {
        SampleProvenanceImpl.SampleProvenanceImplBuilder sp = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_9999")
                .sampleName("TEST_9999_001")
                .iusTag("AAAAAAAA-TTTTTTTT")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("99999")
                .version("version")
                .lastModified(expectedDate);

        List<SampleProvenance> currentList = Lists.newArrayList(spp.getSampleProvenance());
        when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_0001_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);

        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_9999_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void barcodeLengthMismatchTest2() {
        SampleProvenanceImpl.SampleProvenanceImplBuilder sp1 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_9998")
                .sampleName("TEST_9998_001")
                .iusTag("AAAAAAAT")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("99998")
                .version("version")
                .lastModified(expectedDate);

        SampleProvenanceImpl.SampleProvenanceImplBuilder sp2 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_9999")
                .sampleName("TEST_9999_001")
                .iusTag("AAAAAAAA-TTTTTTTT")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("99999")
                .version("version")
                .lastModified(expectedDate);

        List<SampleProvenance> currentList = Lists.newArrayList(spp.getSampleProvenance());
        when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp1.build(), sp2.build()))));

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_9999_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);

        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_0001_001", "TEST_9998_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 5); //2+3

        filters.remove(FileProvenanceFilter.sample);
        EnumMap<FileProvenanceFilter, Set<String>> excludeFilters = new EnumMap<>(FileProvenanceFilter.class);
        excludeFilters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_9999_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setExcludeFilters(excludeFilters); //exclude TEST_9999_001
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8); //2+3+3
    }

    @Test
    public void overrideBaseMaskTruncationTest() {
        bcl2fastqDecider.setOverrideBasesMask(BasesMask.fromString("Y*,I4,Y*"));
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);

        //run on all lanes
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        bcl2fastqDecider.getValidWorkflowRuns().stream().forEach(wr -> {
            assertEquals(wr.getIniFile().get("use_bases_mask"), "y*,I4n*,y*");
            ((WorkflowRunV2) wr).getBcl2FastqData().getSps().forEach((sp) -> {
                assertEquals(sp.getIusTag().length(), 4);
            });
        });
    }

    @Test
    public void overrideBaseMaskInvalidBasesMaskTest() {
        bcl2fastqDecider.setOverrideBasesMask(BasesMask.fromString("Y*,I9,Y*"));
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
    }

    @Test
    public void overrideBaseMaskDuplicateBarcodesTest() {
        SampleProvenanceImpl.SampleProvenanceImplBuilder sp1 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_9998")
                .sampleName("TEST_9998_001")
                .iusTag("AAAAAAAT")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("2")
                .version("version")
                .lastModified(expectedDate);

        SampleProvenanceImpl.SampleProvenanceImplBuilder sp2 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("HiSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_9999")
                .sampleName("TEST_9999_001")
                .iusTag("AAAAAAAG")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("3")
                .version("version")
                .lastModified(expectedDate);

        List<SampleProvenance> currentList = Lists.newArrayList(spp.getSampleProvenance());
        when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp1.build(), sp2.build()))));

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        bcl2fastqDecider.setIncludeFilters(filters);

        //should produce invalid workflow run with duplicate barcode "AAAAAAA"
        bcl2fastqDecider.setOverrideBasesMask(BasesMask.fromString("Y*,I7,Y*"));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 1);

        bcl2fastqDecider.setOverrideBasesMask(BasesMask.fromString("Y*,I8,Y*"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidWorkflowRuns().size(), 0);
    }

    private Collection<FileProvenance> getFpsForCurrentWorkflow() {
        return provenanceClient.getFileProvenance(
                ImmutableMap.<FileProvenanceFilter, Set<String>>of(FileProvenanceFilter.workflow, ImmutableSet.of(bcl2fastqWorkflow.getSwAccession().toString())));
    }

}
