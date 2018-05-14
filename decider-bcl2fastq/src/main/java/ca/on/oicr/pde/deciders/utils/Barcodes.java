package ca.on.oicr.pde.deciders.utils;

import ca.on.oicr.pde.deciders.data.Barcode;
import ca.on.oicr.pde.deciders.data.BasesMask;
import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 *
 * @author mlaszloffy
 */
public class Barcodes {

    public static Barcode applyBasesMask(Barcode barcode, BasesMask basesMask) throws DataMismatchException {

        String barcodeOne = barcode.getBarcodeOne();
        if (basesMask.getIndexOneIncludeLength() != null && basesMask.getIndexOneIncludeLength() > 0 && barcodeOne != null) {
            if (barcodeOne.length() < basesMask.getIndexOneIncludeLength()) {
                // do not modify barcode
            } else {
                barcodeOne = barcodeOne.substring(0, basesMask.getIndexOneIncludeLength());
            }
        }

        String barcodeTwo = barcode.getBarcodeTwo();
        if (basesMask.getIndexTwoIncludeLength() != null && basesMask.getIndexTwoIncludeLength() > 0 && barcodeTwo != null) {
            if (barcodeTwo.length() < basesMask.getIndexTwoIncludeLength()) {
                // do not modify barcode
            } else {
                barcodeTwo = barcodeTwo.substring(0, basesMask.getIndexTwoIncludeLength());
            }
        }

        if (barcodeOne != null && !barcodeOne.isEmpty() && (barcodeTwo == null || barcodeTwo.isEmpty())) {
            return new Barcode(barcodeOne);
        } else if ((barcodeOne == null || barcodeOne.isEmpty()) && barcodeTwo != null && !barcodeTwo.isEmpty()) {
            return new Barcode(barcodeTwo);
        } else if (barcodeOne != null && !barcodeOne.isEmpty() && barcodeTwo != null && !barcodeTwo.isEmpty()) {
            return new Barcode(barcodeOne, barcodeTwo);
        } else {
            throw new DataMismatchException("Unexpected state, barcode one = [" + barcodeOne + "], barcode two = [" + barcodeTwo + "]");
        }

    }

    public static Integer calculateEditDistance(Barcode left, Barcode right) {
        Integer editDistance = 0;
        editDistance += StringUtils.getLevenshteinDistance(Strings.nullToEmpty(left.getBarcodeOne()), Strings.nullToEmpty(right.getBarcodeOne()));
        editDistance += StringUtils.getLevenshteinDistance(Strings.nullToEmpty(left.getBarcodeTwo()), Strings.nullToEmpty(right.getBarcodeTwo()));
        return editDistance;
    }

    public static Integer calculateSimilarity(Barcode target, Barcode other) {
        Integer editDistance = 0;

        String targetBarcodeOne = target.getBarcodeOne();
        String otherBarcodeOne = other.getBarcodeOne();
        if (otherBarcodeOne.length() > targetBarcodeOne.length()) {
            otherBarcodeOne = otherBarcodeOne.substring(0, targetBarcodeOne.length());
        }
        editDistance += StringUtils.getLevenshteinDistance(targetBarcodeOne, otherBarcodeOne);

        String targetBarcodeTwo = Strings.nullToEmpty(target.getBarcodeTwo());
        String otherBarcodeTwo = Strings.nullToEmpty(other.getBarcodeTwo());
        if (otherBarcodeTwo.length() > targetBarcodeTwo.length()) {
            otherBarcodeTwo = otherBarcodeTwo.substring(0, targetBarcodeTwo.length());
        }
        editDistance += StringUtils.getLevenshteinDistance(targetBarcodeTwo, otherBarcodeTwo);

        return editDistance;
    }

    public static List<Pair<Barcode, Barcode>> getCollisions(List<Barcode> barcodes, Integer maxAllowedEditDistance) {
        List<Pair<Barcode, Barcode>> ps = new ArrayList<>();
        for (Barcode target : barcodes) {
            for (Barcode other : barcodes) {
                if (target == other) {
                    continue;
                }
                if (calculateSimilarity(target, other) < maxAllowedEditDistance) {
                    ps.add(Pair.of(target, other));
                }
            }
        }
        return ps;
    }

    public static List<Pair<Barcode, Barcode>> getProblems(List<Barcode> barcodes, Integer maxAllowedEditDistance) {
        List<Pair<Barcode, Barcode>> ps = new ArrayList<>();
        for (int i = 0; i < barcodes.size(); i++) {
            Barcode target = barcodes.get(i);
            for (int j = i + 1; j < barcodes.size(); j++) {
                Barcode other = barcodes.get(j);
                if (calculateEditDistance(target, other) < maxAllowedEditDistance) {
                    ps.add(Pair.of(target, other));
                }
            }
        }
        return ps;
    }

}
