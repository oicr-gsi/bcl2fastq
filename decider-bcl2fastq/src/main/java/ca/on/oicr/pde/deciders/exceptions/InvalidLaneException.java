package ca.on.oicr.pde.deciders.exceptions;

/**
 *
 * @author mlaszloffy
 */
public class InvalidLaneException extends Exception {

    /**
     * Creates a new instance of <code>NewException</code> without detail
     * message.
     */
    public InvalidLaneException() {
    }

    /**
     * Constructs an instance of <code>NewException</code> with the specified
     * detail message.
     *
     * @param msg the detail message.
     */
    public InvalidLaneException(String msg) {
        super(msg);
    }
}
