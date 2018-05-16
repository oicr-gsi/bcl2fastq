package ca.on.oicr.pde.deciders.data;

/**
 *
 * @author mlaszloffy
 */
public class BarcodeCollision {

    private final Barcode barcode;
    private final Barcode collidesWithBarcode;

    public BarcodeCollision(Barcode barcode, Barcode collidesWithBarcode) {
        this.barcode = barcode;
        this.collidesWithBarcode = collidesWithBarcode;
    }

    @Override
    public String toString() {
        return "BarcodeCollision{" + "barcode=" + barcode + ", collidesWithBarcode=" + collidesWithBarcode + '}';
    }

}
