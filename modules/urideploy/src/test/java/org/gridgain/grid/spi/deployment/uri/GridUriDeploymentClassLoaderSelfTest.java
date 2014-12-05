/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.gridgain.grid.spi.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;

import java.net.*;
import java.util.*;

/**
 * Grid URI deployment class loader test.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentClassLoaderSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testNestedJarClassloading() throws Exception {
        ClassLoader ldr = getGarClassLoader();

        // Load class from nested JAR file
        assert ldr.loadClass("javax.mail.Service") != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClasspathResourceLoading() throws Exception {
        ClassLoader ldr = getGarClassLoader();

        // Get resource from GAR file
        URL rsrcUrl = ldr.getResource("org/gridgain/test/test.properties");

        assert rsrcUrl != null;
    }

    /**
     * @return Test GAR's class loader
     * @throws Exception if test GAR wasn't deployed
     */
    private ClassLoader getGarClassLoader() throws Exception {
        DeploymentResource task = getSpi().findResource("GridUriDeploymentTestWithNameTask7");

        assert task != null;

        return task.getClassLoader();
    }

    /**
     * @return List of URIs to use in this test.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList(GridTestProperties.getProperty("ant.urideployment.gar.uri").
            replace("EXTDATA", U.resolveGridGainPath("modules/extdata").getAbsolutePath()));
    }
}
