package ca.on.oicr.pde.deciders.utils;

import ca.on.oicr.pde.deciders.data.Barcode;
import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class BarcodeComparisonTest {

    public BarcodeComparisonTest() {
    }

    @Test
    public void testSomeMethod() throws DataMismatchException {
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAA")), Integer.valueOf(0));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAT")), Integer.valueOf(1));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAAT")), Integer.valueOf(1));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAAT"), Barcode.fromString("AAAA")), Integer.valueOf(1));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(4));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA")), Integer.valueOf(4));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAA-TTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAA-TTT")), Integer.valueOf(2));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AA-TTTT")), Integer.valueOf(2));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("A-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(3));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("A-TTTT")), Integer.valueOf(3));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(8));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("TTTT")), Integer.valueOf(8));

        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAT-TTTA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(Barcodes.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAT-TTTA")), Integer.valueOf(2));
    }

    @Test
    public void testSomeMethod2() throws DataMismatchException {
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA"), Barcode.fromString("AAAA")), Integer.valueOf(0));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA"), Barcode.fromString("AAAT")), Integer.valueOf(1));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA"), Barcode.fromString("AAAAT")), Integer.valueOf(0));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAAT"), Barcode.fromString("AAAA")), Integer.valueOf(1));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA")), Integer.valueOf(4));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAA-TTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAA-TTT")), Integer.valueOf(2));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AA-TTTT")), Integer.valueOf(2));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("A-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("A-TTTT")), Integer.valueOf(3));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(4));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("TTTT")), Integer.valueOf(8));

        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAT-TTTA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(Barcodes.calculateSimilarity(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAT-TTTA")), Integer.valueOf(2));
    }

}
