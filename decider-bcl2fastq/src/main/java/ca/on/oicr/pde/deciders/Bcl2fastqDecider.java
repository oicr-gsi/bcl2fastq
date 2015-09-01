package ca.on.oicr.pde.deciders;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import net.sourceforge.seqware.common.model.Lane;
import net.sourceforge.seqware.common.model.Sample;
import net.sourceforge.seqware.common.model.SequencerRun;
import net.sourceforge.seqware.common.model.WorkflowParam;
import net.sourceforge.seqware.common.module.ReturnValue;
import net.sourceforge.seqware.common.util.Log;
import net.sourceforge.seqware.common.util.filetools.FileTools;
import net.sourceforge.seqware.common.util.filetools.FileTools.LocalhostPair;
import net.sourceforge.seqware.pipeline.plugin.Plugin;
import net.sourceforge.seqware.pipeline.runner.PluginRunner;
import ca.on.oicr.pinery.client.HttpResponseException;
import ca.on.oicr.pinery.client.PineryClient;
import ca.on.oicr.ws.dto.AttributeDto;
import ca.on.oicr.ws.dto.RunDto;
import ca.on.oicr.ws.dto.RunDtoPosition;
import ca.on.oicr.ws.dto.RunDtoSample;
import ca.on.oicr.ws.dto.SampleDto;

public class Bcl2fastqDecider extends Plugin {
	
	private static final String ARG_HELP = "help";
	private static final String ARG_VERBOSE = "verbose";
	private static final String ARG_TEST_MODE = "test";
	private static final String ARG_PINERY_URL = "pinery-url";
	private static final String ARG_NO_META = "no-metadata";
	private static final String ARG_RUN_NAME = "run-name";
	private static final String ARG_RUN_DIR = "run-dir";
	private static final String ARG_READ_ENDS = "read-ends";
	private static final String ARG_WORKFLOW_ACCESSION = "wf-accession";
	private static final String ARG_OUT_PATH = "output-path";
	private static final String ARG_OUT_FOLDER = "output-folder";
	private static final String ARG_MANUAL_OUT = "manual-output";
	private static final String ARG_OFFLINE_BCL = "do-olb";
	private static final String ARG_MISSING_BCL = "ignore-missing-bcl";
	private static final String ARG_MISSING_STATS = "ignore-missing-stats";
	private static final String ARG_INSECURE_PINERY = "insecure-pinery";
	
	private String pineryUrl = null;
	
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
	private final Set<Integer> laneAccessions = new HashSet<>();
	private int readEnds = 2; // TODO: Get from Pinery/SeqWare instead of requiring default/parameter
	
	private boolean insecurePinery = false;
	private boolean testing = false;
	
	
	public Bcl2fastqDecider() {
		super();
		parser.accepts(ARG_HELP, "Prints this help message.");
		parser.accepts(ARG_VERBOSE, "Optional: Log all SeqWare information.");
		
		parser.accepts(ARG_TEST_MODE, "Optional: Testing mode. Prints the INI files to standard out and does not submit the workflow.");
		
		parser.accepts(ARG_PINERY_URL, "Required: Pinery webservice URL.").withRequiredArg();
		
		parser.accepts(ARG_WORKFLOW_ACCESSION, "Required: The workflow accession of the workflow").withRequiredArg();
		parser.accepts(ARG_RUN_NAME, "Required: The sequencer run to process").withRequiredArg();
		parser.accepts(ARG_RUN_DIR, "Required: The sequencer run directory").withRequiredArg();
		parser.accepts(ARG_READ_ENDS, "Optional: Must specify 1 if single end. Defaults to 2 (paired end)").withRequiredArg();
		
		parser.accepts(ARG_NO_META, "Optional: a flag that prevents metadata writeback (which is done "
                + "by default) by the Decider and that is subsequently "
                + "passed to the called workflow which can use it to determine if "
                + "they should write metadata at runtime on the cluster.");
		
		parser.accepts(ARG_OUT_PATH, "Optional: The absolute path of the directory to put the final file(s) (workflow output-prefix option).").withRequiredArg();
		parser.accepts(ARG_OUT_FOLDER, "Optional: The relative path to put the final result(s) (workflow output-dir option).").withRequiredArg();
		parser.accepts(ARG_MANUAL_OUT, "Optional: Set output path manually.");
		
		parser.accepts(ARG_OFFLINE_BCL, "Optional: Perform offline base calling.");
		parser.accepts(ARG_MISSING_BCL, "Optional: Passed on to Bustard.");
		parser.accepts(ARG_MISSING_STATS, "Optional: Passed on to Bustard.");
		parser.accepts(ARG_INSECURE_PINERY, "Optional: Ignore certificate and hostname errors from Pinery.");
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
		if (!this.options.has(ARG_WORKFLOW_ACCESSION) || this.options.valueOf(ARG_WORKFLOW_ACCESSION) == null || 
				this.options.valueOf(ARG_WORKFLOW_ACCESSION).toString().isEmpty()) {
			return missingParameter(ARG_WORKFLOW_ACCESSION);
		}
		else if (!this.options.has(ARG_RUN_NAME) || this.options.valueOf(ARG_RUN_NAME) == null || 
				this.options.valueOf(ARG_RUN_NAME).toString().isEmpty()) {
			return missingParameter(ARG_RUN_NAME);
		}
		else if (!this.options.has(ARG_RUN_DIR) || this.options.valueOf(ARG_RUN_DIR) == null || 
				this.options.valueOf(ARG_RUN_DIR).toString().isEmpty()) {
			return missingParameter(ARG_RUN_DIR);
		}
		else if (!this.options.has(ARG_PINERY_URL) || this.options.valueOf(ARG_PINERY_URL) == null || 
				this.options.valueOf(ARG_PINERY_URL).toString().isEmpty()) {
			return missingParameter(ARG_PINERY_URL);
		}
		else {
			this.pineryUrl = this.options.valueOf(ARG_PINERY_URL).toString();
            this.workflowAccession = options.valueOf(ARG_WORKFLOW_ACCESSION).toString();
            this.runName = this.options.valueOf(ARG_RUN_NAME).toString();
            this.runDir = this.options.valueOf(ARG_RUN_DIR).toString();
            if (!this.runDir.endsWith("/")) runDir += "/";
        }
		
		// Optional args
		if (this.options.has(ARG_READ_ENDS)) {
			String ends = this.options.valueOf(ARG_READ_ENDS).toString().trim();
			if ("1".equals(ends)) this.readEnds = 1;
			else if ("2".equals(ends)) this.readEnds = 2;
			else {
				Log.error("Invalid argument. Value of read-ends must be 1 or 2.");
				return new ReturnValue(ReturnValue.INVALIDARGUMENT);
			}
		}
		
		if (this.options.has(ARG_NO_META)) this.metadataWriteback = false;
		
		if (this.options.has(ARG_OUT_PATH)) {
			this.outPath = this.options.valueOf(ARG_OUT_PATH).toString();
			if (!this.outPath.endsWith("/")) outPath += "/";
		}
		if (this.options.has(ARG_OUT_FOLDER)) this.outFolder = this.options.valueOf(ARG_OUT_FOLDER).toString();
		if (this.options.has(ARG_MANUAL_OUT)) this.manualOutput = true;
		
		if (this.options.has(ARG_OFFLINE_BCL)) this.offlineBcl = true;
		if (this.options.has(ARG_MISSING_BCL)) this.ignoreMissingBcl = true;
		if (this.options.has(ARG_MISSING_STATS)) this.ignoreMissingStats = true;
		
		if (this.options.has(ARG_INSECURE_PINERY)) this.insecurePinery = true;
		if (this.options.has(ARG_TEST_MODE)) this.testing = true;
		
		return new ReturnValue(ReturnValue.SUCCESS);
	}
	
	private ReturnValue missingParameter(String parameter) {
		Log.error("Required parameter missing: "+parameter);
		return new ReturnValue(ReturnValue.INVALIDARGUMENT);
	}
	
	@Override
	public ReturnValue do_run() {
		if (!getLaneInfo()) return new ReturnValue(ReturnValue.PROGRAMFAILED);
		
		String ini = createIniFile(getIniParameters());
		if (ini == null) return new ReturnValue(ReturnValue.PROGRAMFAILED);
		
		return launchWorkflow(ini);
	}
	
	/**
	 * Builds the ini lanes String. Format: {@code <lane>,<lane-swid>:<barcode>,<sample-swid><sample-name>
	 * +<parent-barcode>,<parent-swid>,<parent-name>+...|<lane>...}
	 * 
	 * @return true if successful; false otherwise. If the method fails, a message stating the cause will be logged
	 */
	private boolean getLaneInfo() {
		StringBuilder sb = new StringBuilder();
		
		// Get sequencer run from Pinery
		try (PineryClient pinery = new PineryClient(this.pineryUrl, insecurePinery)) {
			RunDto run = null;
			try {
				Log.debug("Getting sequencer run from Pinery");
				run = pinery.getSequencerRun().byName(this.runName);
			} catch (HttpResponseException ex) {
				Log.fatal("Retrieval of sequencer run from Pinery failed.", ex);
				return false;
			}
			
			// Get run and lanes from SeqWare
			SequencerRun swRun = metadata.getSequencerRunByName(this.runName);
			List<Lane> swLanes = metadata.getLanesFrom(swRun.getSwAccession());
			
			for (RunDtoPosition lane : run.getPositions()) {
				Log.debug("Adding lane "+lane.getPosition());
				Integer laneSwid = getLaneSwid(lane, swLanes);
				if (laneSwid == null) {
					Log.fatal("Could not find lane SWID in SeqWare");
					return false;
				}
				laneAccessions.add(laneSwid);
				
				Set<RunDtoSample> samples = lane.getSamples();
				for (RunDtoSample runSample : samples) {
					Log.debug("Adding run sample "+runSample.getId());
					sb.append(lane.getPosition())
							.append(",")
							.append(laneSwid)
							.append(":");
					int sampleId = runSample.getId();
					boolean first = true;
					try {
						while (sampleId > 0) {
							// Get sample from Pinery
							SampleDto sample = pinery.getSample().byId(sampleId);
							if (first) {
								if (sample.getSampleType() == null || !sample.getSampleType().matches("^Illumina .+ Library Seq$")) {
									Log.fatal("Invalid sample type. Sample type must be 'Illumina * Library Seq' but sample " +
											sample.getName() + " has sample type " +
											(sample.getSampleType() == null ? "null" : sample.getSampleType()));
									return false;
								}
								first = false;
							}
							
							String barcode = getBarcode(sample);
							
							// Get sample swid from SeqWare
							Integer sampleSwid = getSampleSwid(sample);
							if (sampleSwid == null) {
								Log.fatal("Could not find sample SWID in SeqWare");
								return false;
							}
							
							sb.append(barcode)
									.append(",")
									.append(sampleSwid)
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
			this.lanesString = sb.toString();
		}
		return true;
	}
	
	/**
	 * Given a lane from Pinery and a list of lanes from SeqWare, finds the SWID matching the lane from Pinery
	 * 
	 * @param lane lane from Pinery
	 * @param swLanes list of SeqWare lanes (one of which corresponds to the Pinery lane)
	 * @return the SWID, or null if it can't be found
	 */
	private static Integer getLaneSwid(RunDtoPosition lane, List<Lane> swLanes) {
		for (Lane l : swLanes) {
			// Lane index in SeqWare is 0-based, while position in LIMS is 1-based
			if (l.getLaneIndex()+1 == lane.getPosition().intValue()) {
				return l.getSwAccession();
			}
		}
		return null;
	}
	
	/**
	 * Looks up a sample in SeqWare to find its SWID
	 * 
	 * @param sample Sample from Pinery
	 * @return the sample SWID, or null if the sample isn't found
	 */
	private Integer getSampleSwid(SampleDto sample) {
		List<Sample> swSamples = metadata.getSampleByName(sample.getName());
		if (swSamples == null || swSamples.isEmpty()) {
			return null;
		}
		
		return swSamples.get(0).getSwAccession();
	}
	
	private static String getBarcode(SampleDto sample) {
		final String noIndex = "NoIndex";
		final String barcodeAttribute = "Barcode";
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
		return barcode == null ? noIndex : barcode;
	}
	
	/**
	 * Writes the workflow ini file
	 * 
	 * @param iniParameters map of parameters generated by this decider run
	 * @return path of output ini file
	 */
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
	
	/**
	 * Put all parameters in a map to be written to the ini file
	 * 
	 * @return map of all parameters
	 */
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
		run.addProperty("read_ends", String.valueOf(this.readEnds));
		
		return run.getIniFile();
	}
	
	/**
	 * Launches the workflow using the provided ini file. If the test option was provided, the 
	 * workflow will not actually be launched
	 * 
	 * @param iniFile ini file containing workflow parameters
	 * @return
	 */
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
            return new ReturnValue(ReturnValue.INVALIDPARAMETERS);
        }
		runArgs.add("--host");
		runArgs.add(localhost);
		
		runArgs.add("--link-workflow-run-to-parents");
		runArgs.add(commaSeparate(laneAccessions));
		
		StringBuilder sb = new StringBuilder();
		for (String a : runArgs) {
			sb.append(a).append(" ");
		}
		Log.debug("PluginRunner params: " + sb.toString());
		
		if (testing) {
			Log.stdout("Test mode: not launching workflow");
		}
		else {
			Log.stdout("Scheduling workflow run.");
	        PluginRunner.main(runArgs.toArray(new String[runArgs.size()]));
		}
        
		return new ReturnValue(ReturnValue.SUCCESS);
	}
	
	private String commaSeparate(Collection<Integer> things) {
		if (things == null || things.isEmpty()) return "";
		StringBuilder sb = new StringBuilder();
		for (Integer i : things) {
			sb.append(i).append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
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
