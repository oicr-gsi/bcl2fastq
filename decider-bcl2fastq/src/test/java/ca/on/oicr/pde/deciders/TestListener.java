package ca.on.oicr.pde.deciders;

import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

/**
 *
 * @author mlaszloffy
 */
public class TestListener extends TestListenerAdapter {

    @Override
    public void onTestFailure(ITestResult tr) {
        Throwable th = tr.getThrowable();
        if (th != null) {
            th.printStackTrace();
        }
    }
}
