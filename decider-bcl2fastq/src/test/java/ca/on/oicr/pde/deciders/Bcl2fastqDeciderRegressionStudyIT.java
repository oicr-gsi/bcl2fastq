package ca.on.oicr.pde.deciders;

import ca.on.oicr.gsi.provenance.DefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.ExtendedProvenanceClient;
import ca.on.oicr.gsi.provenance.ProvenanceClient;
import ca.on.oicr.gsi.provenance.SeqwareMetadataAnalysisProvenanceProvider;
import ca.on.oicr.gsi.provenance.SeqwareMetadataLimsMetadataProvenanceProvider;
import ca.on.oicr.gsi.provenance.model.FileProvenanceParam;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.pde.client.SeqwareClient;
import ca.on.oicr.pde.testing.metadata.RegressionTestStudy;
import ca.on.oicr.pde.reports.WorkflowReport;
import ca.on.oicr.pde.reports.WorkflowRunReport;
import ca.on.oicr.pde.testing.metadata.SeqwareTestEnvironment;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sourceforge.seqware.common.err.NotFoundException;
import net.sourceforge.seqware.common.metadata.Metadata;
import net.sourceforge.seqware.common.model.IUS;
import net.sourceforge.seqware.common.model.LimsKey;
import net.sourceforge.seqware.common.model.Workflow;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.testng.annotations.BeforeClass;

/**
 *
 * @author mlaszloffy
 */
@Test(singleThreaded = true)
public class Bcl2fastqDeciderRegressionStudyIT {

    private RegressionTestStudy r;
    private ProvenanceClient provenanceClient;
    private ExtendedProvenanceClient extendedProvenanceClient;
    private SeqwareClient seqwareClient;
    private SeqwareTestEnvironment testEnv;
    private Metadata metadata;
    private Map<String, String> config;

    @BeforeClass
    public void setupMetadb() {

        //get the database settings
        String dbHost = System.getProperty("dbHost");
        String dbPort = System.getProperty("dbPort");
        String dbUser = System.getProperty("dbUser");
        String dbPassword = System.getProperty("dbPassword");
        assertNotNull(dbHost, "Set dbHost to a testing Postgres database host name");
        assertNotNull(dbPort, "Set dbPort to a testing Postgres database port");
        assertNotNull(dbUser, "Set dbUser to a testing Postgres database user name");

        //get the seqware webservice war
        String seqwareWarPath = System.getProperty("seqwareWar");
        assertNotNull(seqwareWarPath, "seqwareWar is not set.");
        File seqwareWar = new File(seqwareWarPath);
        assertTrue(seqwareWar.exists(), "seqware was is not accessible.");

        testEnv = new SeqwareTestEnvironment(dbHost, dbPort, dbUser, dbPassword, seqwareWar);

        //get the regression test study and PDE's service objects
        r = new RegressionTestStudy(testEnv.getSeqwareLimsClient());
        seqwareClient = testEnv.getSeqwareClient();
//        seqwareExecutor = testEnv.getSeqwareExecutor();
//        seqwareObjects = r.getSeqwareObjects();

        metadata = testEnv.getMetadata();
        config = testEnv.getSeqwareConfig();

        SeqwareMetadataLimsMetadataProvenanceProvider seqwareMetadataLimsMetadataProvenanceProvider = new SeqwareMetadataLimsMetadataProvenanceProvider(testEnv.getMetadata());
        SeqwareMetadataAnalysisProvenanceProvider seqwareMetadataAnalysisProvenanceProvider = new SeqwareMetadataAnalysisProvenanceProvider(testEnv.getMetadata());
        DefaultProvenanceClient dpc = new DefaultProvenanceClient();
        dpc.registerAnalysisProvenanceProvider("seqware", seqwareMetadataAnalysisProvenanceProvider);
        dpc.registerSampleProvenanceProvider("seqware", seqwareMetadataLimsMetadataProvenanceProvider);
        dpc.registerLaneProvenanceProvider("seqware", seqwareMetadataLimsMetadataProvenanceProvider);
        provenanceClient = dpc;
        extendedProvenanceClient = dpc;
    }

    @Test
    public void basicScheduleTest() throws IOException {
        Workflow bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.8", "", ImmutableMap.of("metadata", "metadata"));

        runDecider(bcl2fastqWorkflow, "--all", "--parent-wf-accession", "0");
        WorkflowReport report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getMaxInputFiles().intValue(), 0);
        Assert.assertEquals(report.getMinInputFiles().intValue(), 0);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 14);

        //all fastqs have been scheduled for processing, no new workflow runs should be schedulable
        runDecider(bcl2fastqWorkflow, "--all", "--parent-wf-accession", "0");
        WorkflowReport report2 = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report2.getWorkflowRunCount().intValue(), 14);
        Assert.assertEquals(report, report2);
    }

    @Test
    public void launchMaxTest() throws IOException {
        Workflow bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.8", "", ImmutableMap.of("metadata", "metadata"));

        WorkflowReport report;

        //schedule 4 runs
        runDecider(bcl2fastqWorkflow, "--all", "--launch-max", "4", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 4);

        //schedule 4 more runs
        runDecider(bcl2fastqWorkflow, "--all", "--launch-max", "4", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 8);

        //shouldn't schedule anything
        runDecider(bcl2fastqWorkflow, "--all", "--launch-max", "0", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 8);

        //schedule the rest
        runDecider(bcl2fastqWorkflow, "--all", "--launch-max", "20", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 14);
    }

    @Test
    public void iusSkipTest() throws IOException {
        Map<String, Set<String>> filters = new HashMap<>();
        filters.put(FileProvenanceParam.sample.toString(), ImmutableSet.of("TEST_0001_Ly_R_PE_500_WG"));
        filters.put(FileProvenanceParam.sequencer_run.toString(), ImmutableSet.of("TEST_SEQUENCER_RUN_001"));
        filters.put(FileProvenanceParam.lane.toString(), ImmutableSet.of("1"));
        Collection<SampleProvenance> sps = provenanceClient.getSampleProvenance(filters);
        SampleProvenance sp = Iterables.getOnlyElement(sps);

        //add a lims key, no associated analysis
        IUS ius = seqwareClient.addLims("seqware", sp.getSampleProvenanceId(), sp.getVersion(), sp.getLastModified());

        Workflow bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.8", "", ImmutableMap.of("metadata", "metadata"));
        WorkflowReport report;

        //schedule workflow runs for TEST_SEQUENCER_RUN_001, an ius is linked to a sample in lane 1, so lane 1 should not be scheduled
        runDecider(bcl2fastqWorkflow, "--sequencer-run-name", "TEST_SEQUENCER_RUN_001", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertFalse(report.getLanes().contains("TEST_SEQUENCER_RUN_001_lane_1"));
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 7);

        //test that --ignore-previous-runs only ignores analysis
        runDecider(bcl2fastqWorkflow, "--sequencer-run-name", "TEST_SEQUENCER_RUN_001", "--lane-number", "1", "--ignore-previous-runs", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertFalse(report.getLanes().contains("TEST_SEQUENCER_RUN_001_lane_1"));
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 7);

        //test --ignore-previous-lims-keys allows the decider to schedule workflow runs for TEST_SEQUENCER_RUN_001 lane 1
        runDecider(bcl2fastqWorkflow, "--sequencer-run-name", "TEST_SEQUENCER_RUN_001", "--lane-number", "1", "--ignore-previous-lims-keys", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertTrue(report.getLanes().contains("TEST_SEQUENCER_RUN_001_lane_1"));
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 8);

        //test --ignore-previous-lims-keys allows the decider to schedule workflow runs for TEST_SEQUENCER_RUN_001
        runDecider(bcl2fastqWorkflow, "--sequencer-run-name", "TEST_SEQUENCER_RUN_001", "--ignore-previous-lims-keys", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 16);

        //remove the IUS-LimsKey that was initially created
        LimsKey lk = metadata.getLimsKeyFrom(ius.getSwAccession());
        ius.setLimsKey(new LimsKey());
        metadata.updateIUS(ius);
        metadata.deleteIUS(ius.getSwAccession());
        metadata.deleteLimsKey(lk.getSwAccession());
        try {
            metadata.getIUS(ius.getSwAccession());
            fail("Expected ius to be deleted");
        } catch (NotFoundException ex) {
        }
        try {
            metadata.getLimsKey(lk.getSwAccession());
            fail("Expected lims key to be deleted");
        } catch (NotFoundException ex) {
        }

        //schedule new workflow runs for TEST_SEQUENCER_RUN_001, all 8 lanes should be processed now that the IUS-LimsKey has been removed
        Workflow bcl2fastqWorkflow2 = seqwareClient.createWorkflow("CASAVA", "2.8", "", ImmutableMap.of("metadata", "metadata"));
        runDecider(bcl2fastqWorkflow2, "--sequencer-run-name", "TEST_SEQUENCER_RUN_001", "--parent-wf-accession", "0");
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow2);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 8);
    }

    @Test
    public void studyToOutputPathTest() throws IOException {
        Workflow bcl2fastqWorkflow = seqwareClient.createWorkflow("CASAVA", "2.8", "", ImmutableMap.of("metadata", "metadata"));

        WorkflowReport report;

        //schedule all runs
        runDecider(bcl2fastqWorkflow,
                "--all",
                "--parent-wf-accession", "0",
                "--study-to-output-path-csv", FileUtils.toFile(this.getClass().getClassLoader().getResource("test-study-to-output-path.csv")).getAbsolutePath()
        );
        report = WorkflowReport.generateReport(seqwareClient, provenanceClient, bcl2fastqWorkflow);
        Assert.assertEquals(report.getWorkflowRunCount().intValue(), 14);

        for (WorkflowRunReport wrr : report.getWorkflowRuns()) {
            assertEquals(wrr.getWorkflowIni().get("output_prefix"), "/tmp/PDE_TEST/");
        }
    }

    private void runDecider(Workflow workflow, String... extraDeciderParams) throws IOException {
        Bcl2fastqDecider decider = new Bcl2fastqDecider();
        decider.setProvenanceClient(extendedProvenanceClient);
        decider.setWorkflowAccession(workflow.getSwAccession().toString());
        run(decider, Arrays.asList(ArrayUtils.toArray(extraDeciderParams)));
    }

    private void run(OicrDecider decider, List<String> params) {
        decider.setMetadata(metadata);
        decider.setConfig(config);
        decider.setParams(params);
        decider.parse_parameters();
        decider.init();
        decider.do_test();
        decider.do_run();
        decider.clean_up();
    }

    @AfterClass
    public void cleanUp() {
        testEnv.shutdown();
    }

}
