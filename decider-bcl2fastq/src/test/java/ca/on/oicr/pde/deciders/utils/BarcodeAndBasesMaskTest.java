package ca.on.oicr.pde.deciders.utils;

import ca.on.oicr.pde.deciders.data.Barcode;
import ca.on.oicr.pde.deciders.data.BasesMask;
import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class BarcodeAndBasesMaskTest {

    @Test
    public void applyBasesMaskOrFailTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,n*,i*,y*")).toString(), "TTTT");
        assertEquals(BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i*,n*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i*,i*,y*")).toString(), "AAAA-TTTT");
        assertEquals(BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA-AAAA"), BasesMask.fromStringUnchecked("y*,i2,i2,y*")).toString(), "AA-AA");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void applyBasesMaskOrFailFailureTest1() throws DataMismatchException {
        BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,n*,i*,y*"));
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void applyBasesMaskOrFailFailureTest2() throws DataMismatchException {
        BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i*,i*,y*"));
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void applyBasesMaskOrFailFailureTest3() throws DataMismatchException {
        BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i*,i*,y*"));
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void applyBasesMaskOrFailFailureTest4() throws DataMismatchException {
        BarcodeAndBasesMask.applyBasesMaskOrFail(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i2,i2,y*"));
    }

    @Test
    public void applyBasesMaskTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i*,y*")).toString(), "AAAA");

        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,n*,i*,y*")).toString(), "TTTT");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i*,n*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i*,i*,y*")).toString(), "AAAA-TTTT");

        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i2,n*,y*")).toString(), "AA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,n*,i2,y*")).toString(), "TT");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromStringUnchecked("y*,i2,i2,y*")).toString(), "AA-TT");

        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i*,i*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i2,i*,y*")).toString(), "AA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,i2,i2,y*")).toString(), "AA");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void applyBasesMaskFailureTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromStringUnchecked("y*,n*,i*,y*")).toString(), "AAAA");
    }

    @Test
    public void calculateBasesMaskTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA")).toString(), "y*,i8n*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("A-AAAAAAAA")).toString(), "y*,i1n*,i8n*,y*");
    }

    @Test
    public void calculateBasesMaskWithRunBasesMaskTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i*,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i*,i*,y*")).toString(), "y*,i8n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i*,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i*,i*,y*")).toString(), "y*,i8n*,i8n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i8,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i8,i8,y*")).toString(), "y*,i8n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i8,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i8,i8,y*")).toString(), "y*,i8n*,i8n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i6,y*")).toString(), "y*,i6n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i6,i6,y*")).toString(), "y*,i6n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i6,y*")).toString(), "y*,i6n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i6,n*,y*")).toString(), "y*,i6n*,n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,n*,i6,y*")).toString(), "y*,n*,i6n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromStringUnchecked("y*,i6,i6,y*")).toString(), "y*,i6n*,i6n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-TTTTTTTT"), BasesMask.fromStringUnchecked("y*,i*,n*,y*")).toString(), "y*,i8n*,n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-TTTTTTTT"), BasesMask.fromStringUnchecked("y*,i6,n*,y*")).toString(), "y*,i6n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-TTTTTTTT"), BasesMask.fromStringUnchecked("y*,n*,i*,y*")).toString(), "y*,n*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-TTTTTTTT"), BasesMask.fromStringUnchecked("y*,n*,i6,y*")).toString(), "y*,n*,i6n*,y*");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void calculateBasesMaskExpectedFailTest() throws DataMismatchException {
        BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromStringUnchecked("y*,n*,i*,y*"));
    }

    @Test
    public void dualBarcodeIgnoreSecondBarcodeTest() throws DataMismatchException {
        Barcode barcode = Barcode.fromString("TAAGGCGA-TATCCTCT");
        BasesMask overrideRunBasesMask = BasesMask.fromStringUnchecked("y*,i*,n*,y*");

        Barcode expectedBarcode = Barcode.fromString("TAAGGCGA");
        BasesMask expectedBasesMask = BasesMask.fromStringUnchecked("y*,i8n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.applyBasesMask(barcode, overrideRunBasesMask), expectedBarcode);
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(barcode, overrideRunBasesMask), expectedBasesMask);
    }

    @Test
    public void dualBarcodeIgnoreFirstBarcodeTest() throws DataMismatchException {
        Barcode barcode = Barcode.fromString("TAAGGCGA-TATCCTCT");
        BasesMask overrideRunBasesMask = BasesMask.fromStringUnchecked("y*,n*,i*,y*");

        Barcode expectedBarcode = new Barcode(null, "TATCCTCT");
        BasesMask expectedBasesMask = BasesMask.fromStringUnchecked("y*,n*,i8n*,y*");

        assertEquals(BarcodeAndBasesMask.applyBasesMask(barcode, overrideRunBasesMask), expectedBarcode);
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(barcode, overrideRunBasesMask), expectedBasesMask);
    }
}
