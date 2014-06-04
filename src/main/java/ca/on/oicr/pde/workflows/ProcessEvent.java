package ca.on.oicr.pde.workflows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author mlaszloffy
 */
public class ProcessEvent {

    private final String laneNumber;
    private final String laneSwAccession;
    private final String barcode;
    private final String iusSwAccession;
    private final String sampleName;
    private final String groupId;


    public ProcessEvent(String laneNumber, String laneSwAccession, String barcode, String iusSwAccession, String sampleName, String groupId) {
        this.laneNumber = laneNumber;
        this.laneSwAccession = laneSwAccession;
        this.barcode = barcode;
        this.iusSwAccession = iusSwAccession;
        this.sampleName = sampleName;
	this.groupId = groupId;
    }


    public String getLaneNumber() {
        return laneNumber;
    }

    public String getLaneSwAccession() {
        return laneSwAccession;
    }

    public String getBarcode() {
        return barcode;
    }

    public String getIusSwAccession() {
        return iusSwAccession;
    }

    public String getSampleName() {
        return sampleName;
    }

    public String getGroupId() {
	return groupId;
    }

    @Override
    public String toString() {
        return String.format("[%s, %s, %s, %s, %s, %s]", laneNumber, laneSwAccession, barcode, iusSwAccession, sampleName, groupId);
    }

    public static List<ProcessEvent> parseLanesString(String lanes) {
        List<ProcessEvent> result = new ArrayList<ProcessEvent>();
        for (String lane : Arrays.asList(lanes.split("\\|"))) {
            String laneInfo = lane.substring(0, lane.indexOf(":"));
            String laneNumber = laneInfo.substring(0, laneInfo.indexOf(","));
            String laneSwAccession = laneInfo.substring(laneInfo.indexOf(",") + 1);
            for (String b : Arrays.asList(lane.substring(lane.indexOf(":") + 1).split("\\+"))) {
                String[] barcodeAttrs = b.split(",");
                String barcode = barcodeAttrs[0];
                String iusSwAccession = barcodeAttrs[1];
                String sampleName = barcodeAttrs[2];
		String groupId = "NoGroup";
		if (barcodeAttrs.length>3)
		    groupId = barcodeAttrs[3];
                result.add(new ProcessEvent(laneNumber, laneSwAccession, barcode, iusSwAccession, sampleName,groupId));
            }
        }
        return result;
    }

    public static List<String> getUniqueSetOfLaneNumbers(List<ProcessEvent> ps) {
        Set<String> laneNumbers = new TreeSet<String>(); //treeset = sorted + distinct elements
        for (ProcessEvent p : ps) {
            laneNumbers.add(p.getLaneNumber());
        }
        return new ArrayList<String>(laneNumbers);
    }

    public static List<ProcessEvent> getProcessEventListFromLaneNumber(List<ProcessEvent> ps, String laneNumber) {
        List<ProcessEvent> result = new ArrayList<ProcessEvent>();
        for (ProcessEvent p : ps) {
            if (p.getLaneNumber().equals(laneNumber)) {
                result.add(p);
            }
        }
        return result;
    }

    public static String getBarcodesStringFromProcessEventList(List<ProcessEvent> ps) {
        StringBuilder sb = new StringBuilder();
        for (ProcessEvent p : ps) {
            sb.append(p.getBarcode()).append(",").append(p.getIusSwAccession()).append(",").append(p.getSampleName()).append("_").append(p.getGroupId());
            sb.append("+");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String getLaneSwid(List<ProcessEvent> ps, String laneNumber) {

        for (ProcessEvent p : ps) {
            if (p.getLaneNumber().equals(laneNumber)) {
                return p.laneSwAccession;
            }
        }

        return null;

    }

    public static boolean containsBarcode(List<ProcessEvent> ps, String barcode) {

        for (ProcessEvent p : ps) {
            if (p.getBarcode().equals(barcode)) {
                return true;
            }
        }

        return false;

    }

}
