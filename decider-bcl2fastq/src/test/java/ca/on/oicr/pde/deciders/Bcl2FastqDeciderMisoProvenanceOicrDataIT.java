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
public class Bcl2FastqDeciderMisoProvenanceOicrDataIT {

    @Factory
    public Object[] oicrDataTest() throws IOException {
        //temporary fix until this is properly handled
        System.setProperty("provenanceSettingsPath", System.getProperty("misoProvenanceSettingsPath"));

        DeciderRunTestFactory d = new DeciderRunTestFactory();
        Object[] tests = d.createTests("target/test-classes/miso-tests.json");
        return tests;
    }

}
