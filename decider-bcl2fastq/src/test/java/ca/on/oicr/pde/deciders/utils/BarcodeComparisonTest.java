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
    public void calculateEditDistanceTest() throws DataMismatchException {
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAA")), Integer.valueOf(0));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAT")), Integer.valueOf(1));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAAT")), Integer.valueOf(1));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAAT"), Barcode.fromString("AAAA")), Integer.valueOf(1));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("ATCGT"), Barcode.fromString("TATCG")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("TATCG"), Barcode.fromString("ATCGT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("ATCGT"), Barcode.fromString("TATCGG")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("TATCGG"), Barcode.fromString("ATCGT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("ATCGT-AAAAA"), Barcode.fromString("TATCGG-AAAAA")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("TATCGG-AAAAA"), Barcode.fromString("ATCGT-AAAAA")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAAA-ATCGT"), Barcode.fromString("AAAAA-TATCGG")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAAA-TATCGG"), Barcode.fromString("AAAAA-ATCGT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(4));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA")), Integer.valueOf(4));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAA-TTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAA-TTT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AA-TTTT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("A-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(3));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("A-TTTT")), Integer.valueOf(3));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(8));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("TTTT")), Integer.valueOf(8));

        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAT-TTTA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateEditDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAT-TTTA")), Integer.valueOf(2));
    }

    @Test
    public void calculateTruncatedHammingDistanceTest() throws DataMismatchException {
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAA")), Integer.valueOf(0));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAT")), Integer.valueOf(1));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAAT")), Integer.valueOf(0));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAAT"), Barcode.fromString("AAAA")), Integer.valueOf(1));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("ATCGT"), Barcode.fromString("TATCG")), Integer.valueOf(5));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("TATCG"), Barcode.fromString("ATCGT")), Integer.valueOf(5));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("ATCGT"), Barcode.fromString("TATCGG")), Integer.valueOf(5));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("TATCGG"), Barcode.fromString("ATCGT")), Integer.valueOf(6));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("ATCGT-AAAAA"), Barcode.fromString("TATCGG-AAAAA")), Integer.valueOf(5));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("TATCGG-AAAAA"), Barcode.fromString("ATCGT-AAAAA")), Integer.valueOf(6));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAAA-ATCGT"), Barcode.fromString("AAAAA-TATCGG")), Integer.valueOf(5));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAAA-TATCGG"), Barcode.fromString("AAAAA-ATCGT")), Integer.valueOf(6));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA")), Integer.valueOf(4));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAA-TTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAA-TTT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AA-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AA-TTTT")), Integer.valueOf(2));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("A-TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(0));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("A-TTTT")), Integer.valueOf(3));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("TTTT"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(4));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("TTTT")), Integer.valueOf(8));

        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAT-TTTA"), Barcode.fromString("AAAA-TTTT")), Integer.valueOf(2));
        assertEquals(BarcodeComparison.calculateTruncatedHammingDistance(Barcode.fromString("AAAA-TTTT"), Barcode.fromString("AAAT-TTTA")), Integer.valueOf(2));
    }

}
