/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 *
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentConfigSelfTest extends GridSpiAbstractConfigTest<GridUriDeploymentSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridUriDeploymentSpi(), "uriList", null);
        checkNegativeSpiProperty(new GridUriDeploymentSpi(), "uriList", Collections.singletonList("qwertyuiop"), false);
        checkNegativeSpiProperty(new GridUriDeploymentSpi(), "uriList", Collections.singletonList(null), false);
    }
}
