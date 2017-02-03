package ca.on.oicr.pde.deciders.handlers;

/**
 *
 * @author mlaszloffy
 */
public interface Handler {

    public boolean isHandlerFor(String workflowName, String workflowVersion);

}
