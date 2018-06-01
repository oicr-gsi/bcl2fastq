package ca.on.oicr.pde.deciders.data;

import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class BarcodeTest {

    public BarcodeTest() {
    }

    @Test
    public void singleBarcodeTest() throws DataMismatchException {
        assertEquals(Barcode.fromString("AAAA").getBarcodeOne(), "AAAA");
        assertNull(Barcode.fromString("AAAA").getBarcodeTwo());
        assertEquals(Barcode.fromString("ATCG").getBarcodeOne(), "ATCG");
        assertNull(Barcode.fromString("ATCG").getBarcodeTwo());
        assertEquals(Barcode.fromString("aaaa").getBarcodeOne(), "AAAA");
        assertNull(Barcode.fromString("aaaa").getBarcodeTwo());
        assertEquals(Barcode.fromString("aaaA").getBarcodeOne(), "AAAA");
        assertNull(Barcode.fromString("aaaA").getBarcodeTwo());
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void singleBarcodeFailure1Test() throws DataMismatchException {
        Barcode.fromString("AAAAB");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void singleBarcodeFailure2Test() throws DataMismatchException {
        Barcode.fromString("AAAA-");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void singleBarcodeFailure3Test() throws DataMismatchException {
        Barcode.fromString(" AAAA ");
    }

    @Test
    public void dualBarcodeTest() throws DataMismatchException {
        assertEquals(Barcode.fromString("AAAA-TTTTT").getBarcodeOne(), "AAAA");
        assertEquals(Barcode.fromString("AAAA-TTTTT").getBarcodeTwo(), "TTTTT");
        assertEquals(Barcode.fromString("ATCG-ATCGG").getBarcodeOne(), "ATCG");
        assertEquals(Barcode.fromString("ATCG-ATCGG").getBarcodeTwo(), "ATCGG");
        assertEquals(Barcode.fromString("aaaa-ttttt").getBarcodeOne(), "AAAA");
        assertEquals(Barcode.fromString("aaaa-ttttt").getBarcodeTwo(), "TTTTT");
        assertEquals(Barcode.fromString("aaaA-ttttT").getBarcodeOne(), "AAAA");
        assertEquals(Barcode.fromString("aaaA-ttttT").getBarcodeTwo(), "TTTTT");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void dualBarcodeFailure1Test() throws DataMismatchException {
        Barcode.fromString("AAAA-TTTTB");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void dualBarcodeFailure2Test() throws DataMismatchException {
        Barcode.fromString("AAAA-TTTT-");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void dualBarcodeFailure3Test() throws DataMismatchException {
        Barcode.fromString(" AAAA-TTTT ");
    }

}
