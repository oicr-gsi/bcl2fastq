/**
 *  Copyright (C) 2014  Ontario Institute of Cancer Research
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Contact us:
 * 
 *  Ontario Institute for Cancer Research  
 *  MaRS Centre, West Tower
 *  661 University Avenue, Suite 510
 *  Toronto, Ontario, Canada M5G 0A3
 *  Phone: 416-977-7599
 *  Toll-free: 1-866-678-6427
 *  www.oicr.on.ca
**/

package ca.on.oicr.pde.workflows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Holds all of the information for a particular IUS, including the lane number and accession, 
 * IUS barcode and accession, sample name and group id, if it exists. Also provides several
 * static utility methods to process and calculate over lists of ProcessEvents.
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

    /**
     * Uniquifies and sorts the lane numbers in a list of ProcessEvents.
     * @return an ordered array of unique lane numbers.
     **/
    public static List<String> getUniqueSetOfLaneNumbers(List<ProcessEvent> ps) {
        Set<String> laneNumbers = new TreeSet<String>(); //treeset = sorted + distinct elements
        for (ProcessEvent p : ps) {
            laneNumbers.add(p.getLaneNumber());
        }
        return new ArrayList<String>(laneNumbers);
    }

    /**
     * Returns all of the ProcessEvents for a single lane number.
     */
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
