/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.ftp;

import org.gridgain.grid.spi.deployment.uri.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Test for FTP deployment.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridFtpDeploymentTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception  If failed.
     */
    public void testDeployment() throws Exception {
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask3");
        checkTask("GridUriDeploymentTestWithNameTask3");
    }

    /**
     * @return List of deployment sources.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList(GridTestProperties.getProperty("deploy.uri.ftp"));
    }
}
