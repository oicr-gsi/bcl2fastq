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

import net.sourceforge.seqware.common.model.IUS;
import net.sourceforge.seqware.common.model.Lane;
import net.sourceforge.seqware.common.model.SequencerRun;
import net.sourceforge.seqware.common.model.WorkflowParam;
import net.sourceforge.seqware.common.module.ReturnValue;
import net.sourceforge.seqware.common.util.Log;
import net.sourceforge.seqware.common.util.filetools.FileTools;
import net.sourceforge.seqware.common.util.filetools.FileTools.LocalhostPair;
import net.sourceforge.seqware.pipeline.plugin.Plugin;
import net.sourceforge.seqware.pipeline.runner.PluginRunner;
import ca.on.oicr.pde.deciders.model.IusData;
import ca.on.oicr.pde.deciders.model.RunData;
import ca.on.oicr.pde.deciders.model.RunPositionData;
import ca.on.oicr.pinery.client.HttpResponseException;
import ca.on.oicr.pinery.client.PineryClient;
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
	
	private static final String noIndex = "NoIndex";
	
	private PineryClient pinery = null;
	private String localhost = null;
	
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
	
	private Set<Integer> iusAccessions;
	private Set<Integer> laneAccessions;
	private int readEnds = 2; // TODO: Get from Pinery/SeqWare instead of requiring default/parameter
	
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
		String pineryUrl = null;
		boolean insecurePinery = false;
		
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
			pineryUrl = this.options.valueOf(ARG_PINERY_URL).toString();
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
		
		if (this.options.has(ARG_INSECURE_PINERY)) insecurePinery = true;
		if (this.options.has(ARG_TEST_MODE)) this.testing = true;
		
		LocalhostPair localhostPair = FileTools.getLocalhost(options);
        this.localhost = localhostPair.hostname;
        if (localhostPair.returnValue.getExitStatus() != ReturnValue.SUCCESS && localhost == null) {
            Log.error("Could not determine localhost: Return value " + localhostPair.returnValue.getExitStatus());
            return new ReturnValue(ReturnValue.INVALIDPARAMETERS);
        }
		
		this.pinery = new PineryClient(pineryUrl, insecurePinery);
		
		return new ReturnValue(ReturnValue.SUCCESS);
	}
	
	private ReturnValue missingParameter(String parameter) {
		Log.error("Required parameter missing: "+parameter);
		return new ReturnValue(ReturnValue.INVALIDARGUMENT);
	}
	
	@Override
	public ReturnValue do_run() {
		try {
			RunData runData = buildDataStructure();
			findAllIus(runData);
			for (RunPositionData lane : runData.getPositions()) {
				String ini = createIniFile(getIniParameters(lane));
				if (ini == null) return new ReturnValue(ReturnValue.PROGRAMFAILED);
				launchWorkflow(ini);
			}
		} catch (HttpResponseException e) {
			Log.error("Error retrieving data from Pinery", e);
			return new ReturnValue(ReturnValue.PROGRAMFAILED);
		} catch (DataMismatchException e) {
			Log.error(e);
			return new ReturnValue(ReturnValue.PROGRAMFAILED);
		}
		
		return new ReturnValue(ReturnValue.SUCCESS);
	}
	
	/**
	 * Retrieves and combines all of the necessary run data from LIMS and SeqWare
	 * 
	 * @return the RunData
	 * @throws HttpResponseException if there is an error retrieving data from LIMS
	 * @throws DataMismatchException if there is an error matching LIMS data with SeqWare data 
	 */
	private RunData buildDataStructure() throws HttpResponseException, DataMismatchException {
		Log.debug("Getting sequencer run " + runName + " from LIMS");
		RunDto limsRun = pinery.getSequencerRun().byName(runName);
		Log.debug("Getting sequencer run " + runName + " from SeqWare");
		SequencerRun swRun = metadata.getSequencerRunByName(runName);
		
		Log.debug("Getting lanes for sequencer run " + runName + " from SeqWare");
		Collection<Lane> swLanes = metadata.getLanesFrom(swRun.getSwAccession());
		Collection<RunPositionData> positions = new ArrayList<>();
		for (RunDtoPosition pos : limsRun.getPositions()) {
			if (pos.getSamples() == null || pos.getSamples().isEmpty()) continue;
			int posNum = pos.getPosition();
			Lane swLane = null;
			for (Lane lane : swLanes) {
				if (lane.getLaneIndex().intValue()+1 == posNum) {
					swLane = lane;
					break;
				}
			}
			if (swLane == null) {
				throw new DataMismatchException("Could not find lane in SeqWare to match lane " + posNum + " in LIMS");
			}
			Collection<IusData> iusDataList = buildIusData(swLane, pos);
			positions.add(new RunPositionData(posNum, swLane, iusDataList));
		}
		if (positions.isEmpty()) throw new DataMismatchException("No samples found in any lanes");
		for (RunPositionData pos : positions) {
			buildLaneString(pos);
			determineBasesMask(pos);
		}
		return new RunData(limsRun, swRun, positions);
	}
	
	/**
	 * Retrieves and combines all the necessary IUS data from LIMS and SeqWare for a lane, matching LIMS samples to SeqWare IUS 
	 * by barcode
	 * 
	 * @param swLane lane from SeqWare
	 * @param pos lane from LIMS
	 * @return all of the IusData for a lane
	 * @throws HttpResponseException if there is an error retrieving data from LIMS
	 * @throws DataMismatchException if there is an error matching LIMS data with SeqWare data
	 */
	private Collection<IusData> buildIusData(Lane swLane, RunDtoPosition pos) throws HttpResponseException, DataMismatchException {
		Collection<IusData> iusDataList = new ArrayList<>();
		Log.debug("Getting all IUS for lane " + pos.getPosition() + " with SWID " + swLane.getSwAccession() + " from SeqWare");
		List<IUS> swLaneIuss = metadata.getIUSFrom(swLane.getSwAccession());;
		
		for (RunDtoSample laneSample : pos.getSamples()) {
			Log.debug("Getting sample id " + laneSample.getId() + " from lane " + pos.getPosition() + " from LIMS");
			SampleDto limsSample = pinery.getSample().byId(laneSample.getId());
			IUS swIus = null;
			
			String limsBarcode = getLimsBarcode(laneSample);
			if (limsBarcode == null || limsBarcode.isEmpty()) {
				if (pos.getSamples().size() == 1 && swLaneIuss.size() == 1) {
					swIus = swLaneIuss.get(0);
					swIus.setTag(noIndex);
				}
				else {
					throw new DataMismatchException("Failed to match LIMS sample with SeqWare IUS");
				}
				
			}
			else {
				for (IUS swLaneIus : swLaneIuss) {
					if (limsBarcode.equals(swLaneIus.getTag())) {
						swIus = swLaneIus;
						break;
					}
				}
				
				if (swIus == null) {
					throw new DataMismatchException("Could not find IUS in SeqWare with barcode " + limsBarcode + 
							" to match LIMS sample " + limsSample.getName() + " in lane " + pos.getPosition());
				}
			}
			
			iusDataList.add(new IusData(swIus, limsSample));
		}
		
		return iusDataList;
	}
	
	/**
	 * Gets the barcode(s) from a LIMS run sample - either the single barcode, or dual barcode joined by a hyphen
	 * 
	 * @param runSample
	 * @return the sample's barcode in either format "AAAAAAAA" or "AAAAAAAA-CCCCCCCC"
	 */
	private String getLimsBarcode(RunDtoSample runSample) {
		if (runSample.getBarcodeTwo() != null && !runSample.getBarcodeTwo().isEmpty()) {
			return runSample.getBarcode() + "-" + runSample.getBarcodeTwo();
		}
		else {
			return runSample.getBarcode();
		}
	}
	
	/**
	 * Builds the ini lanes String
	 * 
	 * @param lane data structure containing all lane data from LIMS and SeqWare including lane, ius, and samples
	 * @return lanes String in the format: {@code <lane#>,<lane-swid>:<barcode>,<ius-swid>,<sample-name>+<more-barcodes>...}
	 */
	private void buildLaneString(RunPositionData lane) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(lane.getPositionNumber())
		.append(",")
		.append(lane.getSwLane().getSwAccession())
		.append(":");
		for (IusData ius : lane.getIus()) {
			sb.append(ius.getSwIus().getTag())
				.append(",")
				.append(ius.getSwIus().getSwAccession())
				.append(",")
				.append(ius.getLimsSample().getName())
				.append("+");
		}
		sb.deleteCharAt(sb.length()-1);
		
		lane.setLaneString(sb.toString());
	}
	
	private void findAllIus(RunData runData) {
		iusAccessions = new HashSet<>();
		laneAccessions = new HashSet<>();
		for (RunPositionData posData : runData.getPositions()) {
			laneAccessions.add(posData.getSwLane().getSwAccession());
			for (IusData iusData : posData.getIus()) {
				iusAccessions.add(iusData.getSwIus().getSwAccession());
			}
		}
	}
	
	/**
	 * Sets the base mask for a lane based on contained barcodes
	 * 
	 * @param pos lane data
	 * @throws DataMismatchException
	 */
	private void determineBasesMask(RunPositionData pos) throws DataMismatchException {
		boolean dualBarcodes = false;
		int barLength = 0;
		for (IusData ius : pos.getIus()) {
			String tag = ius.getSwIus().getTag();
			if (!noIndex.equals(tag)) {
				if (barLength == 0) {
					dualBarcodes = tag.contains("-");
					barLength = tag.length();
				}
			}
			if (tag.length() != barLength) {
				throw new DataMismatchException("Barcodes in lane " + pos.getPositionNumber() + " have different lengths");
			}
		}
		
		if (dualBarcodes) pos.setBasesMask("y*,I*,I*,y*");
		else if (barLength > 0) pos.setBasesMask("y*,I" + barLength + "n*,y*");
		else pos.setBasesMask("");
	}
	
	/**
	 * Writes the workflow ini file
	 * 
	 * @param iniParameters map of parameters generated by this decider run
	 * @return path of output ini file, or null if there is an IO error writing the INI file
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
	private Map<String, String> getIniParameters(RunPositionData lane) {
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
		run.addProperty("use_bases_mask", lane.getBasesMask());
		
		run.addProperty("output_prefix", this.outPath, "./");
		run.addProperty("output_dir", this.outFolder, "seqware-results");
		run.setManualOutput(this.manualOutput);
		
		run.addProperty("lanes", lane.getLaneString());
		run.addProperty("read_ends", String.valueOf(this.readEnds));
		
		return run.getIniFile();
	}
	
	/**
	 * Launches the workflow using the provided ini file. If the test option was provided, the 
	 * workflow will not actually be launched
	 * 
	 * @param iniFile ini file containing workflow parameters
	 */
	private void launchWorkflow(String iniFile) {
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
		runArgs.add("--host");
		runArgs.add(localhost);
		
		runArgs.add("--link-workflow-run-to-parents");
		runArgs.add(commaSeparate(laneAccessions));
		
		runArgs.add("--link-workflow-run-to-parents");
		runArgs.add(commaSeparate(iusAccessions));
		
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
	}
	
	/**
	 * Combines a collection of IDs into a comma-separated String
	 * 
	 * @param things
	 * @return A comma-separated String of integers. e.g. {@code "123,456,789"}
	 */
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
		pinery.close();
		return new ReturnValue(ReturnValue.SUCCESS);
	}
}
