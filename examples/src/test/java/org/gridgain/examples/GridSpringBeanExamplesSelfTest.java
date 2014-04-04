package org.gridgain.examples;

import org.gridgain.examples.misc.springbean.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Spring bean examples self test.
 */
public class GridSpringBeanExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridSpringBeanHelloWorldExample() throws Exception {
        SpringBeanExample.main(EMPTY_ARGS);
    }
}
