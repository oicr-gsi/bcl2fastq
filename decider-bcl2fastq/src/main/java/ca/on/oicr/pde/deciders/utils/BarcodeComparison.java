package ca.on.oicr.pde.deciders.utils;

import ca.on.oicr.pde.deciders.data.Barcode;
import ca.on.oicr.pde.deciders.data.BarcodeCollision;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.text.similarity.HammingDistance;
import org.apache.commons.text.similarity.LevenshteinDistance;

/**
 *
 * @author mlaszloffy
 */
public class BarcodeComparison {

    private static final LevenshteinDistance LEVENSHTEIN_DISTANCE = LevenshteinDistance.getDefaultInstance();
    private static final HammingDistance HAMMING_DISTANCE = new HammingDistance();

    public static Integer calculateEditDistance(Barcode left, Barcode right) {
        Integer editDistance = 0;
        editDistance += LEVENSHTEIN_DISTANCE.apply(Strings.nullToEmpty(left.getBarcodeOne()), Strings.nullToEmpty(right.getBarcodeOne()));
        editDistance += LEVENSHTEIN_DISTANCE.apply(Strings.nullToEmpty(left.getBarcodeTwo()), Strings.nullToEmpty(right.getBarcodeTwo()));
        return editDistance;
    }

    public static Integer calculateTruncatedHammingDistance(Barcode target, Barcode other) {
        Integer editDistance = 0;

        String targetBarcodeOne = target.getBarcodeOne();
        String otherBarcodeOne = other.getBarcodeOne();
        if (otherBarcodeOne.length() > targetBarcodeOne.length()) {
            otherBarcodeOne = otherBarcodeOne.substring(0, targetBarcodeOne.length());
        } else {
            editDistance += targetBarcodeOne.length() - otherBarcodeOne.length();
            targetBarcodeOne = targetBarcodeOne.substring(0, otherBarcodeOne.length());
        }
        editDistance += HAMMING_DISTANCE.apply(targetBarcodeOne, otherBarcodeOne);

        String targetBarcodeTwo = Strings.nullToEmpty(target.getBarcodeTwo());
        String otherBarcodeTwo = Strings.nullToEmpty(other.getBarcodeTwo());
        if (otherBarcodeTwo.length() > targetBarcodeTwo.length()) {
            otherBarcodeTwo = otherBarcodeTwo.substring(0, targetBarcodeTwo.length());
        } else {
            editDistance += targetBarcodeTwo.length() - otherBarcodeTwo.length();
            targetBarcodeTwo = targetBarcodeTwo.substring(0, otherBarcodeTwo.length());
        }
        editDistance += HAMMING_DISTANCE.apply(targetBarcodeTwo, otherBarcodeTwo);

        return editDistance;
    }

    public static List<BarcodeCollision> getTruncatedHammingDistanceCollisions(List<Barcode> barcodes, Integer minAllowedEditDistance) {
        List<BarcodeCollision> collisions = new ArrayList<>();
        for (Barcode target : barcodes) {
            for (Barcode other : barcodes) {
                if (target == other) {
                    continue;
                }
                if (calculateTruncatedHammingDistance(target, other) < minAllowedEditDistance) {
                    collisions.add(new BarcodeCollision(target, other));
                }
            }
        }
        return collisions;
    }

    public static List<BarcodeCollision> getEditDistanceCollisions(List<Barcode> barcodes, Integer minAllowedEditDistance) {
        List<BarcodeCollision> collisions = new ArrayList<>();
        for (int i = 0; i < barcodes.size(); i++) {
            Barcode target = barcodes.get(i);
            for (int j = i + 1; j < barcodes.size(); j++) {
                Barcode other = barcodes.get(j);
                if (calculateEditDistance(target, other) < minAllowedEditDistance) {
                    collisions.add(new BarcodeCollision(target, other));
                }
            }
        }
        return collisions;
    }

}
