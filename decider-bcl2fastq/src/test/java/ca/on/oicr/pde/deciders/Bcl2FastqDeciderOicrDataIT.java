package ca.on.oicr.pde.deciders;

import ca.on.oicr.pde.testing.DeciderRunTestFactory;
import java.io.IOException;
import org.testng.annotations.Factory;
import org.testng.annotations.Listeners;

/**
 *
 * @author mlaszloffy
 */
@Listeners({ca.on.oicr.pde.testing.testng.TestCaseReporter.class})
public class Bcl2FastqDeciderOicrDataIT {

    @Factory
    public Object[] oicrDataTest() throws IOException {
        DeciderRunTestFactory d = new DeciderRunTestFactory();
        return d.createTests("/tests.json");
    }

}
