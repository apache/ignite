/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Test URI deployment with multiple scanners.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentMultiScannersSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testDeployment() throws Exception {
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");
    }

    /**
     * @return List of deployment sources.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        List<String> uriList = new ArrayList<>();

        // Fake URIs.
        uriList.add(GridTestProperties.getProperty("deploy.uri.ftp"));
        uriList.add(GridTestProperties.getProperty("deploy.uri.http"));

        // One real URI.
        uriList.add(GridTestProperties.getProperty("ant.urideployment.gar.uri").
            replace("EXTDATA", U.resolveGridGainPath("modules/extdata").getAbsolutePath()));

        return uriList;
    }
}
