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
import ca.on.oicr.pde.deciders.data.BasesMask;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;
import ca.on.oicr.pde.deciders.utils.PineryClient;
import ca.on.oicr.pde.deciders.utils.RunScannerClient;
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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.stream.Collectors;
import net.sourceforge.seqware.common.metadata.Metadata;
import net.sourceforge.seqware.common.metadata.MetadataInMemory;
import net.sourceforge.seqware.common.model.Workflow;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.powermock.reflect.Whitebox;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
@Listeners({TestListener.class})
public class Bcl2fastqDeciderTest {

    private SampleProvenanceProvider spp;
    private LaneProvenanceProvider lpp;
    private ProvenanceClient provenanceClient;
    private PineryClient pineryClient;
    private RunScannerClient runScannerClient;
    private Bcl2fastqDecider bcl2fastqDecider;
    private Workflow bcl2fastqWorkflow;
    private ZonedDateTime expectedDate = ZonedDateTime.parse("2016-01-01T00:00:00Z");
    private SeqwareClient seqwareClient;
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

        pineryClient = Mockito.mock(PineryClient.class);
        String pineryResult = "{\n"
                + "  \"state\": \"Completed\",\n"
                + "  \"name\": \"any\",\n"
                + "  \"positions\": [\n"
                + "    {\n"
                + "      \"position\": 3,\n"
                + "      \"pool_created_by_id\": 0\n"
                + "    }]\n"
                + "}";
        when(pineryClient.httpGet(Mockito.any())).thenReturn(Optional.of(pineryResult));
        when(pineryClient.fetch(Mockito.any())).thenCallRealMethod();
        when(pineryClient.getRunStatus(Mockito.any())).thenCallRealMethod();

        runScannerClient = Mockito.mock(RunScannerClient.class);
        String runScannerResult = "{\n"
                + "  \"workflowType\": null\n"
                + "}";
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.of(runScannerResult));
        when(runScannerClient.fetch(Mockito.any())).thenCallRealMethod();
        when(runScannerClient.getRunWorkflowType(Mockito.any())).thenCallRealMethod();

        MultiThreadedDefaultProvenanceClient client = new MultiThreadedDefaultProvenanceClient();
        provenanceClient = client;

        SeqwareMetadataAnalysisProvenanceProvider app = new SeqwareMetadataAnalysisProvenanceProvider(metadata);
        client.registerAnalysisProvenanceProvider(provider, app);
        client.registerLaneProvenanceProvider(provider, lpp);
        client.registerSampleProvenanceProvider(provider, spp);
        bcl2fastqDecider.setProvenanceClient(client);
        bcl2fastqDecider.setPineryClient(pineryClient);
        bcl2fastqDecider.setRunScannerClient(runScannerClient);

        seqwareClient = new MetadataBackedSeqwareClient(metadata, config);
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.7.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);

        File runDir = Files.createTempDir();
        runDir.deleteOnExit();

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

        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sp1, sp2));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lp1, lp2));
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        //all lanes have been scheduled - no new bcl2fastqWorkflow runs expected
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        //rerun everything
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8);

        //all should be blocked again
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8);
    }

    @Test
    public void beforeDateFilter() {
        bcl2fastqDecider.setBeforeDateFilter(expectedDate.minusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setBeforeDateFilter(expectedDate.plusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void afterDateFilter() {
        bcl2fastqDecider.setAfterDateFilter(expectedDate.plusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setAfterDateFilter(expectedDate.minusDays(1));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);
    }

    @Test
    public void includeInstrumentFilter() {
        bcl2fastqDecider.setIncludeInstrumentNameFilter(ImmutableSet.of("does not exist"));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setIncludeInstrumentNameFilter(ImmutableSet.of("h001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sequencer_run, ImmutableSet.of("RUN_0001"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.sequencer_run_platform_model, ImmutableSet.of("HiSeq"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        filters.put(FileProvenanceFilter.study, ImmutableSet.of("TEST_STUDY_1"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        // The above bcl2fastqDecider only creates iusLimsKeys and does not schedule a workflow run.
        // The bcl2fastq decider has been updated to only retieve analysis of type "current workflow swid" and "check-wf-swid".
        // This results in the below case not being blocked.  This case can be re-enabled when analysis provenance filtering supports null.
        //bcl2fastqDecider.setIsDryRunMode(false);
        //bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        //bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        //assertEquals(bcl2fastqDecider.run().size(), 0);
        //assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        //assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        //assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        bcl2fastqDecider.setIsDryRunMode(false);
        bcl2fastqDecider.setDoCreateIusLimsKeys(true);
        bcl2fastqDecider.setDoScheduleWorkflowRuns(true);
        bcl2fastqDecider.setIgnorePreviousLimsKeysMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        Collection<? extends FileProvenance> fps = getFpsForCurrentWorkflow();
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
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2);

        //correct the group ids
        sp2Attrs.clear();
        sp2Attrs.put(Lims.GROUP_ID.getAttributeTitle(), ImmutableSortedSet.of("group2"));

        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));

        assertEquals(bcl2fastqDecider.run().size(), 1);  //lane 2 is okay
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2); //(1+1)

        //correct the barcode and schedule
        sp.iusTag("TTTTTTTT");
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 5); //2+(1+2)
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
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));

        //there is a collison in index 1, nothing should be scheduled
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        //fix the barcode
        sp.iusTag("AAAAATTT-TTTTTTTT");
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp.build()))));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
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
                .iusTag("AAAAATTT")
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
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp1.build(), sp2.build()))));

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        bcl2fastqDecider.setIncludeFilters(filters);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        //correct collision (AAAAAAAA vs. AAAAAAAA-TTTTTTTT)
        sp2.iusTag("AAAAAGGG-TTTTTTTT");
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp1.build(), sp2.build()))));
        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_9999_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 2); //(1+1)

        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_0001_001", "TEST_9998_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 5); //2+(1+2)

        filters.remove(FileProvenanceFilter.sample);
        EnumMap<FileProvenanceFilter, Set<String>> excludeFilters = new EnumMap<>(FileProvenanceFilter.class);
        excludeFilters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_9999_001"));
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setExcludeFilters(excludeFilters); //exclude TEST_9999_001
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 8); //5+(1+2)

        //force rerun everything together
        bcl2fastqDecider.setIncludeFilters(filters);
        bcl2fastqDecider.setExcludeFilters(new EnumMap<>(FileProvenanceFilter.class));
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 13); //8+(1+2)+(1+1)
    }

    @Test
    public void overrideBaseMaskTruncationTest() {
        bcl2fastqDecider.setOverrideRunBasesMask(BasesMask.fromStringUnchecked("Y*,I4,Y*"));
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);

        //run on all lanes
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4);

        bcl2fastqDecider.getValidWorkflowRuns().stream().forEach(wr -> {
            assertEquals(wr.getIniFile().get("use_bases_mask"), "y*,i4n*,y*");
            ((WorkflowRunV2) wr).getBcl2FastqData().getSps().forEach((sp) -> {
                assertEquals(sp.getIusTag().length(), 4);
            });
        });
    }

    @Test
    public void overrideBaseMaskInvalidBasesMaskTest() {
        bcl2fastqDecider.setOverrideRunBasesMask(BasesMask.fromStringUnchecked("Y*,I9,Y*"));
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        bcl2fastqDecider.getValidWorkflowRuns().stream().forEach(wr -> {
            assertEquals(wr.getIniFile().get("use_bases_mask"), "y*,i8n*,y*");
            ((WorkflowRunV2) wr).getBcl2FastqData().getSps().forEach((sp) -> {
                assertEquals(sp.getIusTag().length(), 8);
            });
        });
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
                .iusTag("AAAAATCT")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("10")
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
                .iusTag("AAAAAGGG")
                .sampleAttributes(sp1Attrs)
                .skip(false)
                .provenanceId("11")
                .version("version")
                .lastModified(expectedDate);

        List<SampleProvenance> currentList = Lists.newArrayList(spp.getSampleProvenance());
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Lists.newArrayList(Iterables.concat(currentList, Arrays.asList(sp1.build(), sp2.build()))));

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.lane, ImmutableSet.of("RUN_0001_lane_1"));
        bcl2fastqDecider.setIncludeFilters(filters);

        //should produce invalid workflow run with barcode collisions
        bcl2fastqDecider.setOverrideRunBasesMask(BasesMask.fromStringUnchecked("Y*,I7,Y*"));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 0);

        bcl2fastqDecider.setOverrideRunBasesMask(BasesMask.fromStringUnchecked("Y*,I8,Y*"));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 4); //(1+3)
    }

    @Test
    public void singleSampleNoBarcodeTest() {
        LaneProvenance lp1 = LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0003")
                .laneNumber("1")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of("/tmp/run3/"))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("2_1")
                .version("1")
                .lastModified(expectedDate)
                .build();
        SampleProvenance sp1 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0003")
                .studyTitle("TEST_STUDY_2")
                .laneNumber("1")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("")
                .sampleAttributes(sp2Attrs)
                .skip(false)
                .provenanceId("2")
                .version("1")
                .lastModified(expectedDate)
                .build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sp1));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lp1));

        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 2); // 1 + 1

        SampleProvenance sp2 = SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0003")
                .studyTitle("TEST_STUDY_2")
                .laneNumber("1")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0002")
                .sampleName("TEST_0002_001")
                .iusTag("A")
                .sampleAttributes(sp2Attrs)
                .skip(false)
                .provenanceId("3")
                .version("1")
                .lastModified(expectedDate)
                .build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sp1, sp2));

        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
        assertEquals(getFpsForCurrentWorkflow().size(), 2); // 1 + 1
    }

    @Test
    public void mixedSingleAndDualBarcodeRunTest() {
        Pair<Map<String, LaneProvenanceImpl.LaneProvenanceImplBuilder>, Map<String, SampleProvenanceImpl.SampleProvenanceImplBuilder>> mockData = getMockData();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(mockData.getRight().values().stream().map(s -> s.build()).collect(Collectors.toList()));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(mockData.getLeft().values().stream().map(l -> l.build()).collect(Collectors.toList()));

        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        bcl2fastqDecider.setOverrideRunBasesMask(BasesMask.fromStringUnchecked("y*,i*,i*,y*"));
        assertEquals(bcl2fastqDecider.run().size(), 5);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 5);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 5);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 12); // 1+1 + 1+2 + (1+1 + 1+1) + 1+2
    }

    @Test
    public void runBasesMaskTest() {
        Pair<Map<String, LaneProvenanceImpl.LaneProvenanceImplBuilder>, Map<String, SampleProvenanceImpl.SampleProvenanceImplBuilder>> mockData = getMockData();

        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(mockData.getRight().values().stream().map(s -> s.build()).collect(Collectors.toList()));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(mockData.getLeft().values().stream()
                .map(l -> {
                    l.sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y126,I9,I8,y126"));
                    return l.build();
                }).collect(Collectors.toList()));

        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 5);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 5);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 5);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForCurrentWorkflow().size(), 12); // 1+1 + 1+2 + (1+1 + 1+1) + 1+2
    }

    @Test
    public void noLaneSplittingModeTest() {

        //setup decider
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        bcl2fastqDecider.setDoLaneSplitting(false);

        //case when there are different samples per lane
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        SampleProvenance lane1_sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_1_1").build();
        SampleProvenance lane1_sample2 = getBaseSample().laneNumber("1").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_1_2").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").build();
        SampleProvenance lane2_sample3 = getBaseSample().laneNumber("2").sampleName("TEST_0001_003").iusTag("CCCC").provenanceId("1_2_3").build();
        SampleProvenance lane2_sample4 = getBaseSample().laneNumber("2").sampleName("TEST_0001_004").iusTag("GGGG").provenanceId("1_2_4").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1_sample1, lane1_sample2, lane2_sample3, lane2_sample4));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);

        //fix the samples in lane 2
        lane2_sample3 = getBaseSample().laneNumber("2").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_2_3").build();
        lane2_sample4 = getBaseSample().laneNumber("2").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_2_4").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1_sample1, lane1_sample2, lane2_sample3, lane2_sample4));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertTrue(bcl2fastqDecider.getScheduledWorkflowRuns().stream().map(w -> w.getIniFile().get("no_lane_splitting")).allMatch(s -> "true".equals(s)));

        //removing samples from lane 2 should also be valid
        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true); //need to ignore the above scheduled workflow run
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1_sample1, lane1_sample2));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertTrue(bcl2fastqDecider.getScheduledWorkflowRuns().stream().map(w -> w.getIniFile().get("no_lane_splitting")).allMatch(s -> "true".equals(s)));
    }

    @Test
    public void noLaneSplittingModeSampleOrderTest() {
        //setup decider
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        bcl2fastqDecider.setDoLaneSplitting(false);

        //case when there are different samples per lane
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        SampleProvenance lane1_sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_1_1").build();
        SampleProvenance lane1_sample2 = getBaseSample().laneNumber("1").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_1_2").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").build();
        SampleProvenance lane2_sample3 = getBaseSample().laneNumber("2").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_2_3").build();
        SampleProvenance lane2_sample4 = getBaseSample().laneNumber("2").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_2_4").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1_sample1, lane1_sample2, lane2_sample3, lane2_sample4));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertTrue(bcl2fastqDecider.getScheduledWorkflowRuns().stream().map(w -> w.getIniFile().get("no_lane_splitting")).allMatch(s -> "true".equals(s)));

        //lane 1 is scheduled, lane 2 should be filtered
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
    }

    @Test
    public void noLaneSplittingModeSamplesInWrongLaneTest() {
        //setup decider
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        bcl2fastqDecider.setDoLaneSplitting(false);

        //case when there are different samples per lane
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").build();
        SampleProvenance sample1 = getBaseSample().laneNumber("2").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_2_1").build();
        SampleProvenance sample2 = getBaseSample().laneNumber("2").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_2_2").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sample1, sample2));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);

        //move samples to lane 1
        sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_1_1").build();
        sample2 = getBaseSample().laneNumber("1").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_1_2").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sample1, sample2));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertTrue(bcl2fastqDecider.getScheduledWorkflowRuns().stream().map(w -> w.getIniFile().get("no_lane_splitting")).allMatch(s -> "true".equals(s)));
    }

    @Test
    public void emptyLane() throws IOException {
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1));
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Collections.emptyList());

        //lane split workflowType
        String runScannerResult = "{\n"
                + "  \"workflowType\": \"NovaSeqStandard\"\n"
                + "}";
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.of(runScannerResult));
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
    }

    @Test
    public void runScannerWorkflowTest() throws IOException {
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        SampleProvenance lane1Sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_1_1").build();
        SampleProvenance lane1Sample2 = getBaseSample().laneNumber("1").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_1_2").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").build();
        SampleProvenance lane2Sample1 = getBaseSample().laneNumber("2").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_2_1").build();
        SampleProvenance lane2Sample2 = getBaseSample().laneNumber("2").sampleName("TEST_0001_002").iusTag("TTTT").provenanceId("1_2_2").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1Sample1, lane1Sample2, lane2Sample1, lane2Sample2));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));

        //lane split workflowType
        String runScannerResult = "{\n"
                + "  \"workflowType\": null\n"
                + "}";
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.of(runScannerResult));
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        //non-lane split workflowType
        runScannerResult = "{\n"
                + "  \"workflowType\": \"NovaSeqStandard\"\n"
                + "}";
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.of(runScannerResult));
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        //no workflowType
        runScannerResult = "{\n"
                + "  \"workflowTypeMissing\": \"missing\"\n"
                + "}";
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.of(runScannerResult));
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 2);

        //multiple workflowTypes
        runScannerResult = "{\n"
                + "  \"workflowType\": \"NovaSeqStandard\",\n"
                + "  \"workflowType\": null\n"
                + "}";
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.of(runScannerResult));
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 2);

        //non-200 or no data
        when(runScannerClient.httpGet(Mockito.any())).thenReturn(Optional.empty());
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 2);

        //exception when getting data
        when(runScannerClient.httpGet(Mockito.any())).thenThrow(new IOException());
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 2);
    }

    @Test
    public void test10xFilter() {
        //setup decider
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);

        //case when there are different samples per lane
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").build();
        LaneProvenance lane3 = getBaseLane().laneNumber("3").provenanceId("1_3").build();
        SampleProvenance sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("SI-GA").provenanceId("1_1_1").build();
        SortedMap<String, SortedSet<String>> attrs = new TreeMap<>();
        attrs.put("geo_prep_kit", ImmutableSortedSet.of("A 10X kit"));
        SampleProvenance sample2 = getBaseSample().laneNumber("2").sampleName("TEST_0001_002").iusTag("AAAAAA").sampleAttributes(attrs).provenanceId("1_2_2").build();
        SampleProvenance sample3 = getBaseSample().laneNumber("3").sampleName("TEST_0001_003").iusTag("AAAAAA").provenanceId("1_3_3").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sample1, sample2, sample3));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2, lane3));
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
    }

    @Test
    public void testMixed10xLane() {
        bcl2fastqDecider.setWorkflow(bcl2fastqWorkflow);
        bcl2fastqDecider.setDisableRunCompleteCheck(true);

        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").build();
        SampleProvenance sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("SI-GA").provenanceId("1_1_1").build();
        SampleProvenance sample2 = getBaseSample().laneNumber("1").sampleName("TEST_0001_002").iusTag("AAAAAA").provenanceId("1_2_2").build();

        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(sample1, sample2));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1));

        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        bcl2fastqDecider.getValidWorkflowRuns().stream().forEach(wr -> {
            assertTrue(wr.getIniFile().get("lanes").contains("AAAAAA"));
            List<SampleProvenance> sps = ((WorkflowRunV2) wr).getBcl2FastqData().getSps();
            assertEquals(sps.size(), 1);
        });

        EnumMap<FileProvenanceFilter, Set<String>> filters = new EnumMap<>(FileProvenanceFilter.class);
        filters.put(FileProvenanceFilter.sample, ImmutableSet.of("TEST_0001_001"));
        bcl2fastqDecider.setExcludeFilters(filters);
        bcl2fastqDecider.setIsDemultiplexSingleSampleMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 0);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);

        bcl2fastqDecider.setIgnorePreviousAnalysisMode(true);
        assertEquals(bcl2fastqDecider.run().size(), 1);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
    }

    @Test
    public void testSingleEndRun() {
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y100,i8")).build();
        SampleProvenance lane1_sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("AAAAAAAA").provenanceId("1_1_1").build();
        SampleProvenance lane1_sample2 = getBaseSample().laneNumber("1").sampleName("TEST_0001_002").iusTag("TTTTTTTT").provenanceId("1_1_2").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y100,i8")).build();
        SampleProvenance lane2_sample4 = getBaseSample().laneNumber("2").sampleName("TEST_0001_001").iusTag("AAAA").provenanceId("1_2_3").build();
        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1_sample1, lane1_sample2, lane2_sample4));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2));
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertTrue(bcl2fastqDecider.getScheduledWorkflowRuns().stream().map(w -> w.getIniFile().get("read_ends")).allMatch(s -> "1".equals(s)));
    }

    @Test
    public void testEmptyOrNoIndexBarcode() {
        LaneProvenance lane1 = getBaseLane().laneNumber("1").provenanceId("1_1").sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y*,i*,y*")).build();
        SampleProvenance lane1_sample1 = getBaseSample().laneNumber("1").sampleName("TEST_0001_001").iusTag("NoIndex").provenanceId("1_1_1").build();
        LaneProvenance lane2 = getBaseLane().laneNumber("2").provenanceId("1_2").sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y*,i*,y*")).build();
        SampleProvenance lane2_sample1 = getBaseSample().laneNumber("2").sampleName("TEST_0001_002").iusTag("").provenanceId("1_2_1").build();

        // only one empty or NoIndex barcoded sample is supported per lane
        LaneProvenance lane3 = getBaseLane().laneNumber("3").provenanceId("1_3").sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y*,i*,y*")).build();
        SampleProvenance lane3_sample1 = getBaseSample().laneNumber("3").sampleName("TEST_0001_003").iusTag("NoIndex").provenanceId("1_3_1").build();
        SampleProvenance lane3_sample2 = getBaseSample().laneNumber("3").sampleName("TEST_0002_001").iusTag("").provenanceId("1_3_2").build();

        // a single end lane
        LaneProvenance lane4 = getBaseLane().laneNumber("4").provenanceId("1_4").sequencerRunAttribute("run_bases_mask", ImmutableSortedSet.of("y*,i*")).build();
        SampleProvenance lane4_sample1 = getBaseSample().laneNumber("4").sampleName("TEST_0001_004").iusTag("NoIndex").provenanceId("1_4_1").build();

        Mockito.<Collection<? extends SampleProvenance>>when(spp.getSampleProvenance()).thenReturn(Arrays.asList(lane1_sample1, lane2_sample1, lane3_sample1, lane3_sample2, lane4_sample1));
        Mockito.<Collection<? extends LaneProvenance>>when(lpp.getLaneProvenance()).thenReturn(Arrays.asList(lane1, lane2, lane3, lane4));
        assertEquals(bcl2fastqDecider.run().size(), 3);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 1);
    }

    @Test
    public void testDoNotProvisionOutUndetermined() throws IOException {
        bcl2fastqDecider.setWorkflow(seqwareClient.createWorkflow("CASAVA", "2.7.1", "test workflow"));
        bcl2fastqDecider.setProvisionOutUndetermined(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);

        bcl2fastqDecider.setWorkflow(seqwareClient.createWorkflow("CASAVA", "2.9.1", "test workflow"));
        bcl2fastqDecider.setProvisionOutUndetermined(false);
        assertEquals(bcl2fastqDecider.run().size(), 0);

        Workflow bcl2fastq_2_9_2 = seqwareClient.createWorkflow("CASAVA", "2.9.2", "test workflow");
        bcl2fastqDecider.setWorkflow(bcl2fastq_2_9_2);
        bcl2fastqDecider.setProvisionOutUndetermined(false);
        assertEquals(bcl2fastqDecider.run().size(), 2);
        assertEquals(bcl2fastqDecider.getScheduledWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getValidWorkflowRuns().size(), 2);
        assertEquals(bcl2fastqDecider.getInvalidLanes().size(), 0);
        assertEquals(getFpsForWorkflow(bcl2fastq_2_9_2).size(), 4);
        bcl2fastqDecider.getValidWorkflowRuns().stream().forEach(wr -> {
            assertEquals(wr.getIniFile().get("provision_out_undetermined"), "false");
        });
    }

    private Pair<Map<String, LaneProvenanceImpl.LaneProvenanceImplBuilder>, Map<String, SampleProvenanceImpl.SampleProvenanceImplBuilder>> getMockData() {
        Map<String, LaneProvenanceImpl.LaneProvenanceImplBuilder> lanes = new HashMap<>();
        Map<String, SampleProvenanceImpl.SampleProvenanceImplBuilder> samples = new HashMap();

        LaneProvenanceImpl.LaneProvenanceImplBuilder builder = LaneProvenanceImpl.builder();
        lanes.put("1_1", LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .laneNumber("1")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of("/tmp/run_dir/"))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("1_1")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_1_1", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("1")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_1_1")
                .version("1")
                .lastModified(expectedDate));

        lanes.put("1_2", LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .laneNumber("2")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of("/tmp/run_dir/"))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("1_2")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_2_1", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("2")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA-AAAAATTT")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_2_1")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_2_2", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("2")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA-AAAAAAAA")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_2_2")
                .version("1")
                .lastModified(expectedDate));

        lanes.put("1_3", LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .laneNumber("3")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of("/tmp/run_dir/"))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("1_3")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_3_1", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("3")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA-TTTTTTTT")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_3_1")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_3_2", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("3")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("TTTTTTTT")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_3_2")
                .version("1")
                .lastModified(expectedDate));

        lanes.put("1_4", LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .laneNumber("4")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of("/tmp/run_dir/"))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .skip(false)
                .provenanceId("1_4")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_4_1", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("4")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("AAAAAAAA")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_4_1")
                .version("1")
                .lastModified(expectedDate));
        samples.put("1_4_2", SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .laneNumber("4")
                .sequencerRunPlatformModel("NextSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleName("TEST_0001_001")
                .iusTag("AAAAATTT")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .provenanceId("1_4_2")
                .version("1")
                .lastModified(expectedDate));

        return Pair.of(lanes, samples);
    }

    private LaneProvenanceImpl.LaneProvenanceImplBuilder getBaseLane() {
        return LaneProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .sequencerRunAttribute("run_dir", ImmutableSortedSet.of("/tmp/run_dir/"))
                .sequencerRunAttribute("instrument_name", ImmutableSortedSet.of("n001"))
                .sequencerRunPlatformModel("NovaSeq")
                .createdDate(expectedDate)
                .skip(false)
                .version("1")
                .lastModified(expectedDate);
    }

    private SampleProvenanceImpl.SampleProvenanceImplBuilder getBaseSample() {
        return SampleProvenanceImpl.builder()
                .sequencerRunName("RUN_0001")
                .studyTitle("TEST_STUDY_1")
                .sequencerRunPlatformModel("NovaSeq")
                .createdDate(expectedDate)
                .rootSampleName("TEST_0001")
                .sampleAttributes(Collections.emptySortedMap())
                .skip(false)
                .version("1")
                .lastModified(expectedDate);
    }

    private Collection<? extends FileProvenance> getFpsForWorkflow(Workflow workflow) {
        return provenanceClient.getFileProvenance(
                ImmutableMap.<FileProvenanceFilter, Set<String>>of(
                        FileProvenanceFilter.workflow,
                        ImmutableSet.of(workflow.getSwAccession().toString())
                ));
    }

    private Collection<? extends FileProvenance> getFpsForCurrentWorkflow() {
        return getFpsForWorkflow(bcl2fastqWorkflow);
    }

}
