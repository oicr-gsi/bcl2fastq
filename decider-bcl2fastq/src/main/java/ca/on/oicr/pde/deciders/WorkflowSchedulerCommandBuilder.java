package ca.on.oicr.pde.deciders;

import com.google.common.base.Joiner;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import net.sourceforge.seqware.common.util.filetools.FileTools;

/**
 *
 * @author mlaszloffy
 */
public class WorkflowSchedulerCommandBuilder {

    private final Integer workflowAccession;
    private boolean metadataWriteback = false;
    private String host = null;
    private Path iniFile = null;
    private List<String> overrideArgs = null;
    private List<Integer> iusSwidsToLinkWorkflowRunTo = null;

    public WorkflowSchedulerCommandBuilder(Integer workflowAccession) {
        this.workflowAccession = workflowAccession;
    }

    public void setMetadataWriteback(boolean metadataWriteback) {
        this.metadataWriteback = metadataWriteback;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setIniFile(Path iniFile) {
        this.iniFile = iniFile;
    }

    public void setOverrideArgs(List<String> overrideArgs) {
        this.overrideArgs = overrideArgs;
    }

    public void setIusSwidsToLinkWorkflowRunTo(List<Integer> iusSwidsToLinkWorkflowRunTo) {
        this.iusSwidsToLinkWorkflowRunTo = iusSwidsToLinkWorkflowRunTo;
    }

    public List<String> build() {
        List<String> args = new ArrayList<>();
        args.add("--plugin");
        args.add("io.seqware.pipeline.plugins.WorkflowScheduler");
        args.add("--");
        args.add("--workflow-accession");
        args.add(workflowAccession.toString());
        if (!metadataWriteback) {
            args.add("--no-metadata");
        }
        args.add("--host");
        if (host == null || host.isEmpty()) {
            args.add(FileTools.getLocalhost(null).hostname);
        } else {
            args.add(host);
        }
        if (iniFile != null) {
            args.add("--ini-file");
            args.add(iniFile.toAbsolutePath().toString());
        }
        if (metadataWriteback && iusSwidsToLinkWorkflowRunTo != null && !iusSwidsToLinkWorkflowRunTo.isEmpty()) {
            args.add("--link-workflow-run-to-parents");
            args.add(Joiner.on(",").join(iusSwidsToLinkWorkflowRunTo));
        }
        if (overrideArgs != null && !overrideArgs.isEmpty()) {
            args.add("--");
            args.addAll(overrideArgs);
        }
        return args;
    }

}
