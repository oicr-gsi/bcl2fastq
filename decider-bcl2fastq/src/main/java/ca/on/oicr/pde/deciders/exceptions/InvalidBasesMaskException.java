package ca.on.oicr.pde.deciders.exceptions;

/**
 *
 * @author mlaszloffy
 */
public class InvalidBasesMaskException extends Exception {

    /**
     * Creates a new instance of <code>InvalidBasesMaskException</code> without detail message.
     */
    public InvalidBasesMaskException() {
    }

    /**
     * Constructs an instance of <code>InvalidBasesMaskException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public InvalidBasesMaskException(String msg) {
        super(msg);
    }
}
