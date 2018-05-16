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
    public void applyBasesMaskTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromString("y*,i*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromString("y*,i*,y*")).toString(), "AAAA");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromString("y*,n*,i*,y*")).toString(), "TTTT");
        assertEquals(BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA-TTTT"), BasesMask.fromString("y*,i*,i*,y*")).toString(), "AAAA-TTTT");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void applyBasesMaskFailTest() throws DataMismatchException {
        BarcodeAndBasesMask.applyBasesMask(Barcode.fromString("AAAA"), BasesMask.fromString("y*,n*,i*,y*"));
    }

    @Test
    public void calculateBasesMaskTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA")).toString(), "y*,i8n*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("A-AAAAAAAA")).toString(), "y*,i1n*,i8n*,y*");
    }

    @Test
    public void calculateBasesMaskWithRunBasesMaskTest() throws DataMismatchException {
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,i*,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,i*,i*,y*")).toString(), "y*,i8n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromString("y*,i*,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromString("y*,i*,i*,y*")).toString(), "y*,i8n*,i8n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,i8,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,i8,i8,y*")).toString(), "y*,i8n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromString("y*,i8,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromString("y*,i8,i8,y*")).toString(), "y*,i8n*,i8n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,i6,y*")).toString(), "y*,i6n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,i6,i6,y*")).toString(), "y*,i6n*,n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromString("y*,i6,y*")).toString(), "y*,i6n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-AAAAAAAA"), BasesMask.fromString("y*,i6,i6,y*")).toString(), "y*,i6n*,i6n*,y*");

        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-TTTTTTTT"), BasesMask.fromString("y*,n*,i*,y*")).toString(), "y*,i8n*,y*");
        assertEquals(BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA-TTTTTTTT"), BasesMask.fromString("y*,n*,i6,y*")).toString(), "y*,i6n*,y*");
    }

    @Test(expectedExceptions = DataMismatchException.class)
    public void calculateBasesMaskExpectedFailTest() throws DataMismatchException {
        BarcodeAndBasesMask.calculateBasesMask(Barcode.fromString("AAAAAAAA"), BasesMask.fromString("y*,n*,i*,y*"));
    }
}
