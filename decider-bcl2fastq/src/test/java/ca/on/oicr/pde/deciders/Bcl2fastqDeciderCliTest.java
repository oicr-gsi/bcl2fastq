package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.ProviderLoader;
import ca.on.oicr.gsi.provenance.ProviderLoader.Provider;
import ca.on.oicr.pde.client.MetadataBackedSeqwareClient;
import ca.on.oicr.pde.client.SeqwareClient;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sourceforge.seqware.common.metadata.MetadataInMemory;
import net.sourceforge.seqware.common.model.Workflow;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2fastqDeciderCliTest {

    private Workflow bcl2fastqWorkflow;
    private File provenanceSettings;

    @BeforeMethod
    public void setup() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("SW_METADATA_METHOD", "inmemory");

        Provider p = new Provider();
        p.setProvider("test");
        p.setType("ca.on.oicr.gsi.provenance.SeqwareMetadataLimsMetadataProvenanceProvider");
        p.setProviderSettings(config);
        ProviderLoader pl = new ProviderLoader(Arrays.asList(p));

        provenanceSettings = File.createTempFile("provenance", ".json");
        provenanceSettings.deleteOnExit();
        FileUtils.writeStringToFile(provenanceSettings, pl.getProvidersAsJson(), Charsets.UTF_8, true);

        SeqwareClient seqwareClient = new MetadataBackedSeqwareClient(new MetadataInMemory(), config);
        bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.7.1", "test workflow");
    }

    @Test
    public void dryRunOptTest() throws IOException {
        List<String> args = new ArrayList<>();
        args.add("--dry-run");
        args.add("--wf-accession");
        args.add(bcl2fastqWorkflow.getSwAccession().toString());
        args.add("--provenance-settings");
        args.add(provenanceSettings.getAbsolutePath());
        args.add("--all");
        args.add("--disable-run-complete-check");

        Bcl2fastqDecider decider = getDecider(args);
        assertTrue(decider.getIsDryRunMode());
        assertFalse(decider.getDoMetadataWriteback());
        assertFalse(decider.getDoCreateIusLimsKeys());
        assertFalse(decider.getDoScheduleWorkflowRuns());
        assertFalse(decider.getIsDemultiplexSingleSampleMode());
    }

    @Test
    public void singleSampleModeTest() throws IOException {
        List<String> args = new ArrayList<>();
        args.add("--dry-run");
        args.add("--wf-accession");
        args.add(bcl2fastqWorkflow.getSwAccession().toString());
        args.add("--provenance-settings");
        args.add(provenanceSettings.getAbsolutePath());
        args.add("--all");
        args.add("--demux-single-sample");
        args.add("--disable-run-complete-check");

        Bcl2fastqDecider decider = getDecider(args);
        assertTrue(decider.getIsDryRunMode());
        assertFalse(decider.getDoMetadataWriteback());
        assertFalse(decider.getDoCreateIusLimsKeys());
        assertFalse(decider.getDoScheduleWorkflowRuns());
        assertTrue(decider.getIsDemultiplexSingleSampleMode());
    }

    @Test
    public void afterDateTest() throws IOException {
        List<String> args = new ArrayList<>();
        args.add("--dry-run");
        args.add("--wf-accession");
        args.add(bcl2fastqWorkflow.getSwAccession().toString());
        args.add("--provenance-settings");
        args.add(provenanceSettings.getAbsolutePath());
        args.add("--after-date");
        args.add("2017-01-01");
        args.add("--all");
        args.add("--disable-run-complete-check");

        Bcl2fastqDecider decider = getDecider(args);
        assertTrue(decider.getIsDryRunMode());
        assertFalse(decider.getDoMetadataWriteback());
        assertFalse(decider.getDoCreateIusLimsKeys());
        assertFalse(decider.getDoScheduleWorkflowRuns());
        assertFalse(decider.getIsDemultiplexSingleSampleMode());
        assertTrue(decider.getAfterDateFilter().isEqual(ZonedDateTime.parse("2017-01-01T00:00:00Z")));
    }

    @Test
    public void multipleFilterValuesTest() throws IOException {
        List<String> args = new ArrayList<>();
        args.add("--dry-run");
        args.add("--wf-accession");
        args.add(bcl2fastqWorkflow.getSwAccession().toString());
        args.add("--provenance-settings");
        args.add(provenanceSettings.getAbsolutePath());
        args.add("--include-study");
        args.add("1,2,3");
        args.add("--exclude-study");
        args.add("1,2,3");
        args.add("--disable-run-complete-check");

        Bcl2fastqDecider decider = getDecider(args);
        assertTrue(decider.getIsDryRunMode());
        assertFalse(decider.getDoMetadataWriteback());
        assertFalse(decider.getDoCreateIusLimsKeys());
        assertFalse(decider.getDoScheduleWorkflowRuns());
        assertFalse(decider.getIsDemultiplexSingleSampleMode());
        assertEquals(decider.getIncludeFilters().get(FileProvenanceFilter.study), ImmutableSet.of("1", "2", "3"));
        assertEquals(decider.getExcludeFilters().get(FileProvenanceFilter.study), ImmutableSet.of("1", "2", "3"));
    }

    private Bcl2fastqDecider getDecider(List<String> args) {
        Map<String, String> config = new HashMap<>();
        config.put("SW_METADATA_METHOD", "inmemory");

        Bcl2fastqDeciderCli cli = new Bcl2fastqDeciderCli();
        cli.setMetadata(new MetadataInMemory());
        cli.setConfig(config);
        cli.setParams(args);
        cli.parse_parameters();
        cli.init();
        return cli.getBcl2fastqDecider();
    }

}
