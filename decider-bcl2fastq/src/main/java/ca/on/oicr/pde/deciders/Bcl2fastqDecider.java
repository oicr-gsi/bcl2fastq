package ca.on.oicr.pde.deciders;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import ca.on.oicr.pinery.client.HttpResponseException;
import ca.on.oicr.pinery.client.PineryClient;
import ca.on.oicr.ws.dto.AttributeDto;
import ca.on.oicr.ws.dto.RunDto;
import ca.on.oicr.ws.dto.RunDtoPosition;
import ca.on.oicr.ws.dto.RunDtoSample;
import ca.on.oicr.ws.dto.SampleDto;
import net.sourceforge.seqware.common.model.WorkflowParam;
import net.sourceforge.seqware.common.module.ReturnValue;
import net.sourceforge.seqware.common.util.Log;
import net.sourceforge.seqware.common.util.filetools.FileTools;
import net.sourceforge.seqware.common.util.filetools.FileTools.LocalhostPair;
import net.sourceforge.seqware.pipeline.plugin.Plugin;

public class Bcl2fastqDecider extends Plugin {
	
	private static final String ARG_HELP = "help";
	private static final String ARG_VERBOSE = "verbose";
	private static final String ARG_TEST_MODE = "test";
	private static final String ARG_NO_META = "no-metadata";
	private static final String ARG_RUN_NAME = "run-name";
	private static final String ARG_RUN_DIR = "run-dir";
	private static final String ARG_WORKFLOW_ACCESSION = "wf-accession";
	private static final String ARG_OUT_PATH = "output-path";
	private static final String ARG_OUT_FOLDER = "output-folder";
	private static final String ARG_MANUAL_OUT = "manual-output";
	private static final String ARG_OFFLINE_BCL = "do-olb";
	private static final String ARG_MISSING_BCL = "ignore-missing-bcl";
	private static final String ARG_MISSING_STATS = "ignore-missing-stats";
	
	
	private String workflowAccession = null;
	private String runName = null;
	private String runDir = null;
	
	private boolean metadataWriteback = true;
	
	private String outPath = null;
	private String outFolder = null;
	private boolean manualOutput = false;
	
	private boolean offlineBcl = false;
	private boolean ignoreMissingBcl = false;
	private boolean ignoreMissingStats = false;
	
	private String lanesString = null;
	// TODO: read_ends param
	
	private boolean testing = false;
	
	
	public Bcl2fastqDecider() {
		super();
		parser.accepts(ARG_HELP, "Prints this help message.");
		parser.accepts(ARG_VERBOSE, "Optional: Log all SeqWare information.");
		
		parser.accepts(ARG_TEST_MODE, "Optional: Testing mode. Prints the INI files to standard out and does not submit the workflow.");
		
		parser.accepts(ARG_WORKFLOW_ACCESSION, "Required: The workflow accession of the workflow").withRequiredArg();
		parser.accepts(ARG_RUN_NAME, "Required: The sequencer run to process").withRequiredArg();
		parser.accepts(ARG_RUN_DIR, "Required: The sequencer run directory").withRequiredArg();
		
		parser.accepts(ARG_NO_META, "Optional: a flag that prevents metadata writeback (which is done "
                + "by default) by the Decider and that is subsequently "
                + "passed to the called workflow which can use it to determine if "
                + "they should write metadata at runtime on the cluster.");
		
		parser.accepts(ARG_OUT_PATH, "Optional: The absolute path of the directory to put the final file(s) (workflow output-prefix option).");
		parser.accepts(ARG_OUT_FOLDER, "Optional: The relative path to put the final result(s) (workflow output-dir option).");
		parser.accepts(ARG_MANUAL_OUT, "Optional: Set output path manually.");
		
		parser.accepts(ARG_OFFLINE_BCL, "Optional: Perform offline base calling.");
		parser.accepts(ARG_MISSING_BCL, "Optional: Passed on to Bustard.");
		parser.accepts(ARG_MISSING_STATS, "Optional: Passed on to Bustard.");
	}
	
	@Override
	public ReturnValue init() {
		// Show help
		if (this.options.has(ARG_HELP)) {
			System.err.println(get_syntax());
            return new ReturnValue(ReturnValue.RETURNEDHELPMSG);
		}
		
		if (this.options.has(ARG_VERBOSE)) Log.setVerbose(true);
		Log.debug("INIT");
		
		// Required args
		if (!this.options.has(ARG_WORKFLOW_ACCESSION)) {
			return missingParameter(ARG_WORKFLOW_ACCESSION);
		}
		else if (!this.options.has(ARG_RUN_NAME)) {
			return missingParameter(ARG_RUN_NAME);
		}
		else if (!this.options.has(ARG_RUN_DIR)) {
			return missingParameter(ARG_RUN_DIR);
		}
		else {
            this.workflowAccession = options.valueOf(ARG_WORKFLOW_ACCESSION).toString();
            this.runName = this.options.valueOf(ARG_RUN_NAME).toString();
            this.runDir = this.options.valueOf(ARG_RUN_DIR).toString();
            if (!this.runDir.endsWith("/")) runDir += "/";
        }
		
		// Optional args
		if (this.options.has(ARG_NO_META)) this.metadataWriteback = false;
		
		if (this.options.has(ARG_OUT_PATH)) {
			this.outPath = this.options.valueOf(ARG_OUT_PATH).toString();
			if (!this.outPath.endsWith("/")) outPath += "/";
		}
		if (this.options.has(ARG_OUT_FOLDER)) this.outPath = this.options.valueOf(ARG_OUT_FOLDER).toString();
		if (this.options.has(ARG_MANUAL_OUT)) this.manualOutput = true;
		
		if (this.options.has(ARG_OFFLINE_BCL)) this.offlineBcl = true;
		if (this.options.has(ARG_MISSING_BCL)) this.ignoreMissingBcl = true;
		if (this.options.has(ARG_MISSING_STATS)) this.ignoreMissingStats = true;
		
		if (this.options.has(ARG_TEST_MODE)) this.testing = true;
		
		return new ReturnValue(ReturnValue.SUCCESS);
	}
	
	private ReturnValue missingParameter(String parameter) {
		System.err.println("Required parameter missing: "+parameter);
		return new ReturnValue(ReturnValue.INVALIDARGUMENT);
	}
	
	@Override
	public ReturnValue do_run() {
		if (!getLaneInfo()) return new ReturnValue(ReturnValue.PROGRAMFAILED);
		
		String ini = createIniFile(getIniParameters());
		if (ini == null) return new ReturnValue(ReturnValue.PROGRAMFAILED);
		
		ReturnValue ret = null;
		if (this.testing) {
			Log.debug("Test mode: not launching workflow");
			ret = new ReturnValue(ReturnValue.SUCCESS);
		}
		else {
			ret = launchWorkflow(ini);
		}
		
		return ret;
	}
	
	/**
	 * @return the ini lanes String. Format: {@code <lane>,<lane-swid>:<barcode>,<sample-swid><sample-name>
	 * +<parent-barcode>,<parent-swid>,<parent-name>+...|<lane>...}
	 */
	private boolean getLaneInfo() { // Note: test with 120914_M00753_0025_A000000000-A1M5Y
		final String noIndex = "NoIndex";
		final String barcodeAttribute = "Barcode";
		StringBuilder sb = new StringBuilder();
		
		// TODO: don't hardcode Pinery url
		// TODO: secure https if/when possible
		try (PineryClient pinery = new PineryClient("http://localhost:8888/pinery-ws/", true)) {
			RunDto run = null;
			try {
				Log.debug("Getting all sequencer runs from Pinery");
				List<RunDto> allRuns = pinery.getSequencerRun().all(); // TODO: get individual run by name if/when possible
				for (RunDto r : allRuns) {
					if (this.runName.equals(r.getName())) {
						run = r;
						break;
					}
				}
				if (run == null) {
					Log.fatal("Sequencer run not found in Pinery.");
					return false;
				}
			} catch (HttpResponseException ex) {
				Log.fatal("Retrieval of sequencer runs from Pinery failed.", ex);
				return false;
			}
			
			SampleDto sample = null;
			for (RunDtoPosition lane : run.getPositions()) {
				Log.debug("Adding lane "+lane.getPosition());
				Set<RunDtoSample> samples = lane.getSamples();
				for (RunDtoSample runSample : samples) {
					Log.debug("Adding run sample "+runSample.getId());
					sb.append(lane.getPosition())
							.append(",")
							.append("LaneSWID") // TODO: replace lane swid placeholder
							.append(":");
					int sampleId = runSample.getId();
					try {
						while (sampleId > 0) {
							// TODO: consider getting entire sample list instead of making several calls for single samples
							sample = pinery.getSample().byId(sampleId);
							
							String barcode = null;
							Set<AttributeDto> attributes = sample.getAttributes();
							if (attributes != null) {
								for (AttributeDto a : attributes) {
									if (barcodeAttribute.equals(a.getName())) {
										barcode = a.getValue();
										break;
									}
								}
							}
							if (barcode == null) barcode = noIndex;
							
							sb.append(barcode)
									.append(",")
									.append("SampleSWID") // TODO: replace sample swid placeholder
									.append(",")
									.append(sample.getName())
									.append("+");
							Set<String> parents = sample.getParents();
							if (parents == null || parents.isEmpty()) {
								// Root sample found
								sampleId = -1;
							}
							else if (parents.size() == 1) {
								for (String url : parents) {
									sampleId = Integer.valueOf(url.substring(url.lastIndexOf("/")+1));
									Log.debug("Adding parent sample "+sampleId);
								}
							}
							else {
								Log.fatal("Sample with id "+sampleId+" should have 1 or 0 parents (found "+parents.size()+").");
								return false;
							}
						}
						sb.deleteCharAt(sb.length()-1);
					} catch (HttpResponseException ex) {
						Log.fatal("Retrieval of sample from Pinery failed. Sample id:"+runSample.getId(), ex);
						return false;
					}
					sb.append("|");
				}
			}
			sb.deleteCharAt(sb.length()-1);
		}
		
		this.lanesString = sb.toString();
		return true;
	}
	
	private String createIniFile(Map<String, String> iniParameters) {
		// Note: private method copied over (slightly modified) from BasicDecider
        String iniPath = "";

        Map<String, String> iniFileMap = new TreeMap<>();
        
        // Include defaults from workflow INI
        SortedSet<WorkflowParam> defaultIniParams = metadata.getWorkflowParams(workflowAccession);
        for (WorkflowParam param : defaultIniParams) {
            iniFileMap.put(param.getKey(), param.getDefaultValue());
        }
        
        // Insert decider-modified values
        for (String param : iniParameters.keySet()) {
            iniFileMap.put(param, iniParameters.get(param));
        }
        
        File file = null;
        Random random = new Random(System.currentTimeMillis());
        try {
			file = File.createTempFile("" + random.nextInt(), ".ini");
		} catch (IOException ex) {
			Log.fatal("Failed to create ini file.", ex);
            return null;
		}
        
        try (FileWriter fw = new FileWriter(file);
        		PrintWriter writer = new PrintWriter(fw, true)) {
            for (String key : iniFileMap.keySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append(key).append("=").append(iniFileMap.get(key));
                writer.println(sb.toString());
            }

        } catch (IOException ex) {
            Log.fatal("Failed writing to ini file.", ex);
            return null;
        }
        
        if (file != null) {
            iniPath = file.getAbsolutePath();
        }
        
        Log.debug("INI File written to "+iniPath);
        return iniPath;
    }
	
	private Map<String, String> getIniParameters() {
		WorkflowRun run = new WorkflowRun(null, null);
		
		run.addProperty("java", "/.mounts/labs/seqprodbio/private/seqware/java/default/bin/java");
		run.addProperty("perl", "/usr/bin/perl");
		run.addProperty("tile", "0");
		run.addProperty("threads", "8");
		run.addProperty("memory", "16384");
		
		run.addProperty("flowcell", this.runName);
		run.addProperty("intensity_folder", this.runDir + "Data/Intensities");
		run.addProperty("called_bases", this.runDir + "Data/Intensities/BaseCalls/");
		
		run.addProperty("do_olb", this.offlineBcl ? "1" : "0");
		run.addProperty("ignore_missing_bcl", String.valueOf(this.ignoreMissingBcl));
		run.addProperty("ignore_missing_stats", String.valueOf(this.ignoreMissingStats));
		
		run.addProperty("output_prefix", this.outPath, "./");
		run.addProperty("output_dir", this.outFolder, "seqware-results");
		run.setManualOutput(this.manualOutput);
		
		run.addProperty("lanes", this.lanesString);
		
		return run.getIniFile();
	}
	
	private ReturnValue launchWorkflow(String iniFile) {
		Log.debug("Launching workflow for run "+this.runName+" with INI: "+iniFile);
		
		ArrayList<String> runArgs = new ArrayList<>();
		runArgs.add("--plugin");
		runArgs.add("io.seqware.pipeline.plugins.WorkflowScheduler");
		runArgs.add("--");
		runArgs.add("--workflow-accession");
		runArgs.add(workflowAccession);
		runArgs.add("--ini-files");
		runArgs.add(iniFile);
		if (!metadataWriteback) {
			runArgs.add("--no-metadata");
		}
		
		LocalhostPair localhostPair = FileTools.getLocalhost(options);
        String localhost = localhostPair.hostname;
        if (localhostPair.returnValue.getExitStatus() != ReturnValue.SUCCESS && localhost == null) {
            Log.error("Could not determine localhost: Return value " + localhostPair.returnValue.getExitStatus());
            Log.error("Please supply it on the command line with --host");
            return new ReturnValue(ReturnValue.INVALIDPARAMETERS);
        }
		runArgs.add("--host");
		runArgs.add(localhost);
		
		// TODO: ensure the below parameters are not required
//		Collection<String> fileSWIDs = new ArrayList<>();
//		runArgs.add("--" + WorkflowScheduler.INPUT_FILES);
//		for (Integer fileSWID : fileSWIDsToRun) {
//			fileSWIDs.add(String.valueOf(fileSWID));
//		}
//		runArgs.add(commaSeparateMy(fileSWIDs));
		
//		runArgs.add("--parent-accessions");
//		runArgs.add(commaSeparateMy(parentAccessionsToRun));
//		runArgs.add("--link-workflow-run-to-parents");
//		runArgs.add(commaSeparateMy(workflowParentAccessionsToRun));
//		runArgs.add("--");
//		for (String s : options.valuesOf(nonOptionSpec)) {
//			runArgs.add(s);
//		}
		
		return new ReturnValue(ReturnValue.SUCCESS);
	}

	public static void main(String args[]){

		List<String> params = new ArrayList<String>();
		params.add("--plugin");
		params.add(Bcl2fastqDecider.class.getCanonicalName());
		params.add("--");
		params.addAll(Arrays.asList(args));
		System.out.println("Parameters: " + Arrays.deepToString(params.toArray()));
		net.sourceforge.seqware.pipeline.runner.PluginRunner.main(params.toArray(new String[params.size()]));

	}

	@Override
	public ReturnValue do_test() {
		return new ReturnValue(ReturnValue.NOTIMPLEMENTED);
	}

	@Override
	public ReturnValue clean_up() {
		return new ReturnValue(ReturnValue.NOTIMPLEMENTED);
	}
}
