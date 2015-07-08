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

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class WorkflowClientTest {

    private final String defaultInput = "1,100:NoIndex,101,Sample_Name|"
            + "3,107:AAAGTG,108,Sample_Name+AAAGTC,109,Sample_Name+AAAGTC,110,Sample_Name|"
            + "2,103:TTTGTG,104,Sample_Name+ATTGTC,105,Sample_Name+AATGTG,106,Sample_Name";

    @Test
    public void parseLanesString() {

        String lanes = "6,631732:NoIndex,631733,PCSI_0046_Pa_P_PE_693_WG|"
                + "3,631709:NoIndex,631710,PCSI_0019_Pa_P_PE_562_WG|"
                + "7,631725:NoIndex,631726,PCSI_0072_Pa_P_PE_668_WG|"
                + "2,632022:NoIndex,632023,PCSI_0083_Du_R_PE_589_WG|"
                + "8,631703:NoIndex,631704,PCSI_0309_Ly_R_PE_750_WG|"
                + "1,632020:NoIndex,632021,PCSI_0083_Du_R_PE_593_WG|"
                + "4,631711:NoIndex,631712,PCSI_0047_Pa_P_PE_590_WG|"
                + "5,631723:NoIndex,631724,PCSI_0044_Si_R_PE_716_WG";
        List<ProcessEvent> ps = ProcessEvent.parseLanesString(lanes);
        Assert.assertEquals(ps.size(), 8);

    }

    @Test
    public void getLaneNumbers() {

        String input = "1,100:NoIndex,101,Sample_Name+AAAGTC,102,Sample_Name|"
                + "3,107:NoIndex,108,Sample_Name+AAAGTC,109,Sample_Name+NoIndex,110,Sample_Name|"
                + "3,107:NoIndex,111,Sample_Name+AAAGTC,112,Sample_Name+NoIndex,113,Sample_Name|"
                + "2,103:NoIndex,104,Sample_Name+AAAGTC,105,Sample_Name+NoIndex,106,Sample_Name";
        List<String> expected = Arrays.asList("1", "2", "3");
        List<String> actual = ProcessEvent.getUniqueSetOfLaneNumbers(ProcessEvent.parseLanesString(input));
        Assert.assertEquals(actual, expected);

    }

    @Test
    public void getLaneListFromLaneNumber() {

        String input = "1,100:NoIndex,101,Sample_Name+AAAGTC,102,Sample_Name|"
                + "2,103:NoIndex,104,Sample_Name+AAAGTC,105,Sample_Name+NoIndex,106,Sample_Name|"
                + "1,100:NoIndex,107,Sample_Name+AAAGTC,108,Sample_Name";
        Assert.assertTrue(ProcessEvent.getProcessEventListFromLaneNumber(ProcessEvent.parseLanesString(input), "1").size() == 4);
        Assert.assertTrue(ProcessEvent.getProcessEventListFromLaneNumber(ProcessEvent.parseLanesString(input), "2").size() == 3);
        Assert.assertTrue(ProcessEvent.getProcessEventListFromLaneNumber(ProcessEvent.parseLanesString(input), "3").isEmpty());

    }

    @Test
    public void getBarcodeStringFromLaneList() {

        String input = "1,100:NoIndex,101,Sample_Name+AAAGTC,102,Sample_Name|"
                + "2,103:NoIndex,104,Sample_Name+AAAGTC,105,Sample_Name+NoIndex,106,Sample_Name|"
                + "1,100:NoIndex,107,Sample_Name+AAAGTC,108,Sample_Name";
        String expected = "NoIndex,101,Sample_Name_NoGroup+AAAGTC,102,Sample_Name_NoGroup+NoIndex,107,Sample_Name_NoGroup+AAAGTC,108,Sample_Name_NoGroup";
        String actual = ProcessEvent.getBarcodesStringFromProcessEventList(ProcessEvent.getProcessEventListFromLaneNumber(ProcessEvent.parseLanesString(input), "1"));
        Assert.assertEquals(actual, expected);

    }

    @Test
    public void parseGroupIdFromString() {
	String[] barcodes =	{"AAAA", "AAAT", "AATT", "ATTT", "TTTT"};
	String[] ius = 		{"1111", "1110", "1100", "1000", "0000"};
	String[] groupId =	{"15", "14", "12", "8", "0"};
	String sampleName = "SampleName";

        String input = 	"1,100:";
	for (int i =0; i< barcodes.length; i++) {
	    input += barcodes[i] +","+ ius[i] +","+ sampleName+","+groupId[i];
	    if (i!=barcodes.length-1)
		input += "+";
	}
        List<ProcessEvent> pes =  ProcessEvent.parseLanesString(input);

	for (int i =0; i< barcodes.length; i++) {
	    String expected = "[1, 100, " + barcodes[i] +", "+ ius[i] +", "+ sampleName+", "+groupId[i]+"]";
	    String actual = pes.get(i).toString();
	    Assert.assertEquals(actual, expected);
	}	
       

    }


    @Test
    public void outputString() {

        String expected = "/tmp/Unaligned_110916_SN804_0064_AD04TBACXX_1/Project_na/"
                + "Sample_SWID_9858_PCSI_0106_Ly_R_PE_190_WG_NoGroup_110916_SN804_0064_AD04TBACXX/"
                + "SWID_9858_PCSI_0106_Ly_R_PE_190_WG_NoGroup_110916_SN804_0064_AD04TBACXX_NoIndex_L001_R1_001.fastq.gz";
        String dataDir = "/tmp/";
        String flowcell = "110916_SN804_0064_AD04TBACXX";
        String laneNum = "1";
        String iusSwAccession = "9858";
        String sampleName = "PCSI_0106_Ly_R_PE_190_WG";
        String barcode = "NoIndex";
        String read = "1";
	String groupId = "NoGroup";
        String actual = WorkflowClient.generateOutputPath(dataDir, flowcell, laneNum, iusSwAccession, sampleName, barcode, read, groupId);
        Assert.assertEquals(actual, expected);

    }

    @Test
    public void testFindBarcode() {

        List<ProcessEvent> ps = ProcessEvent.parseLanesString(defaultInput);

        Assert.assertTrue(ProcessEvent.containsBarcode(ps, "TTTGTG"));
        Assert.assertFalse(ProcessEvent.containsBarcode(ps, "ZZZZZZ"));

    }

    @Test
    public void getLaneAccession() {

        List<ProcessEvent> ps = ProcessEvent.parseLanesString(defaultInput);

        Assert.assertEquals(ProcessEvent.getLaneSwid(ps, "1"), "100");
        Assert.assertEquals(ProcessEvent.getLaneSwid(ps, "2"), "103");
        Assert.assertEquals(ProcessEvent.getLaneSwid(ps, "3"), "107");
        Assert.assertEquals(ProcessEvent.getLaneSwid(ps, "100"), null);

    }
}
