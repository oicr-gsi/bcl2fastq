package ca.on.oicr.pde.deciders.exceptions;

/**
 *
 * @author mlaszloffy
 */
public class ConfigurationException extends RuntimeException {

    /**
     * Creates a new instance of <code>ConfigurationException</code> without detail message.
     */
    public ConfigurationException() {
    }

    /**
     * Constructs an instance of <code>ConfigurationException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public ConfigurationException(String msg) {
        super(msg);
    }
}
