package ca.on.oicr.pde.deciders;

/**
 * Exception to represent errors in data that doesn't exist or match expectations
 *
 * @author dcooke
 *
 */
public class DataMismatchException extends Exception {

    private static final long serialVersionUID = -6915118283212652723L;

    public DataMismatchException() {
        super();
    }

    public DataMismatchException(String message) {
        super(message);
    }

    public DataMismatchException(Throwable cause) {
        super(cause);
    }

    public DataMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataMismatchException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
