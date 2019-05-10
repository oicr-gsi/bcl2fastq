package ca.on.oicr.pde.deciders.utils;

import ca.on.oicr.pde.deciders.data.Barcode;
import ca.on.oicr.pde.deciders.data.BasesMask;
import ca.on.oicr.pde.deciders.exceptions.DataMismatchException;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author mlaszloffy
 */
public class BarcodeAndBasesMask {

    public static Barcode applyBasesMaskOrFail(Barcode barcode, BasesMask basesMask) throws DataMismatchException {
        return BarcodeAndBasesMask.applyBasesMask(barcode, basesMask, true);
    }

    public static Barcode applyBasesMask(Barcode barcode, BasesMask basesMask) throws DataMismatchException {
        return BarcodeAndBasesMask.applyBasesMask(barcode, basesMask, false);
    }

    private static Barcode applyBasesMask(Barcode barcode, BasesMask basesMask, boolean strict) throws DataMismatchException {

        String barcodeOne = barcode.getBarcodeOne();
        String barcodeTwo = barcode.getBarcodeTwo();

        if ((barcodeOne == null || barcodeOne.isEmpty()) && (barcodeTwo == null || barcodeTwo.isEmpty())) {
            // Empty or NoIndex barcode, no need to apply bases mask
            return barcode;
        }

        if (basesMask.getIndexOneIncludeLength() == null && barcodeOne == null) {
            barcodeOne = null;
        } else if (basesMask.getIndexOneIncludeLength() == null && barcodeOne != null) {
            barcodeOne = null;
        } else if (basesMask.getIndexOneIncludeLength() != null && barcodeOne == null) {
            if (strict) {
                throw new DataMismatchException("Barcode one missing but bases mask requires it");
            }
        } else if (basesMask.getIndexOneIncludeLength() != null && barcodeOne != null) {
            if (barcodeOne.length() < basesMask.getIndexOneIncludeLength()) {
                // do not modify barcode
            } else {
                barcodeOne = barcodeOne.substring(0, basesMask.getIndexOneIncludeLength());
            }
        }

        if (basesMask.getIndexTwoIncludeLength() == null && barcodeTwo == null) {
            barcodeTwo = null;
        } else if (basesMask.getIndexTwoIncludeLength() == null && barcodeTwo != null) {
            barcodeTwo = null;
        } else if (basesMask.getIndexTwoIncludeLength() != null && barcodeTwo == null) {
            if (strict) {
                throw new DataMismatchException("Barcode two missing but bases mask requires it");
            }
        } else if (basesMask.getIndexTwoIncludeLength() != null && barcodeTwo != null) {
            if (barcodeTwo.length() < basesMask.getIndexTwoIncludeLength()) {
                // do not modify barcode
            } else {
                barcodeTwo = barcodeTwo.substring(0, basesMask.getIndexTwoIncludeLength());
            }
        }

        if (barcodeOne != null && !barcodeOne.isEmpty() && (barcodeTwo == null || barcodeTwo.isEmpty())) {
            return new Barcode(barcodeOne);
        } else if ((barcodeOne == null || barcodeOne.isEmpty()) && barcodeTwo != null && !barcodeTwo.isEmpty()) {
            return new Barcode(null, barcodeTwo);
        } else if (barcodeOne != null && !barcodeOne.isEmpty() && barcodeTwo != null && !barcodeTwo.isEmpty()) {
            return new Barcode(barcodeOne, barcodeTwo);
        } else {
            throw new DataMismatchException("Unexpected state, barcode one = [" + barcodeOne + "], barcode two = [" + barcodeTwo + "]");
        }

    }

    public static BasesMask calculateBasesMask(Barcode barcode) {

        BasesMask.BasesMaskBuilder basesMaskBuilder = new BasesMask.BasesMaskBuilder();

        basesMaskBuilder.setReadOneIncludeLength(Integer.MAX_VALUE);

        if (barcode.getBarcodeOne() != null && barcode.getBarcodeOne().length() > 0) {
            basesMaskBuilder.setIndexOneIncludeLength(barcode.getBarcodeOne().length());
            basesMaskBuilder.setIndexOneIgnoreLength(Integer.MAX_VALUE);
        } else {
            basesMaskBuilder.setIndexOneIgnoreLength(Integer.MAX_VALUE);
        }

        if (barcode.getBarcodeTwo() != null && barcode.getBarcodeTwo().length() > 0) {
            basesMaskBuilder.setIndexTwoIncludeLength(barcode.getBarcodeTwo().length());
            basesMaskBuilder.setIndexTwoIgnoreLength(Integer.MAX_VALUE);
        }

        basesMaskBuilder.setReadTwoIncludeLength(Integer.MAX_VALUE);

        return basesMaskBuilder.createBasesMask();
    }

    public static BasesMask calculateBasesMask(Barcode barcode, BasesMask runBasesMask) throws DataMismatchException {
        Barcode sequencedBarcode = applyBasesMask(barcode, runBasesMask);
        BasesMask barcodeBasesMask = calculateBasesMask(sequencedBarcode);

        BasesMask.BasesMaskBuilder basesMaskBuilder = new BasesMask.BasesMaskBuilder();

        //read one
        basesMaskBuilder.setReadOneIncludeLength(runBasesMask.getReadOneIncludeLength());
        basesMaskBuilder.setReadOneIgnoreLength(runBasesMask.getReadOneIgnoreLength());

        //index one
        if (barcodeBasesMask.getIndexOneIncludeLength() != null) {
            basesMaskBuilder.setIndexOneIncludeLength(barcodeBasesMask.getIndexOneIncludeLength());
            basesMaskBuilder.setIndexOneIgnoreLength(barcodeBasesMask.getIndexOneIgnoreLength());
        }
        if (runBasesMask.getIndexOneIgnoreLength() != null) {
            basesMaskBuilder.setIndexOneIgnoreLength(runBasesMask.getIndexOneIgnoreLength());
        } else {
            basesMaskBuilder.setIndexOneIgnoreLength(Integer.MAX_VALUE);
        }

        //index two
        if (runBasesMask.getIndexTwoIncludeLength() != null) {
            if (barcodeBasesMask.getIndexTwoIncludeLength() == null) {
                if (runBasesMask.getIndexOneIncludeLength() != null) {
                    basesMaskBuilder.setIndexTwoIgnoreLength(Integer.MAX_VALUE);
                } else {
                    basesMaskBuilder.setIndexTwoIgnoreLength(null);
                }
            } else {
                basesMaskBuilder.setIndexTwoIncludeLength(barcodeBasesMask.getIndexTwoIncludeLength());
                basesMaskBuilder.setIndexTwoIgnoreLength(barcodeBasesMask.getIndexTwoIgnoreLength());
            }
        }
        if (runBasesMask.getIndexTwoIgnoreLength() != null) {
            basesMaskBuilder.setIndexTwoIgnoreLength(runBasesMask.getIndexTwoIgnoreLength());
        }

        //read two
        basesMaskBuilder.setReadTwoIncludeLength(runBasesMask.getReadTwoIncludeLength());
        basesMaskBuilder.setReadTwoIgnoreLength(runBasesMask.getReadTwoIgnoreLength());

        return basesMaskBuilder.createBasesMask();
    }

    public static BasesMask calculateBasesMask(Collection<Barcode> barcodes) throws DataMismatchException {
        Set<BasesMask> bs = barcodes.stream().map(b -> calculateBasesMask(b)).collect(Collectors.toSet());
        if (bs.size() != 1) {
            throw new DataMismatchException("Expected one bases mask for a barcode set, found: [" + bs.toString() + "]");
        }
        return Iterables.getOnlyElement(bs);
    }

    public static BasesMask calculateBasesMask(List<Barcode> barcodes, BasesMask runBasesMask) throws DataMismatchException {
        final List<String> errors = new ArrayList<>();
        Set<BasesMask> bs = barcodes.stream().map(b -> {
            try {
                return calculateBasesMask(b, runBasesMask);
            } catch (DataMismatchException ex) {
                errors.add(ex.toString());
                return null;
            }
        }).collect(Collectors.toSet());

        if (!errors.isEmpty()) {
            throw new DataMismatchException("There were errors calculating bases mask for:\n" + Joiner.on("\n").join(errors));
        }
        if (bs.size() != 1) {
            throw new DataMismatchException("Expected one bases mask for a barcode set, found: [" + bs.toString() + "]");
        }
        return Iterables.getOnlyElement(bs);
    }

}
