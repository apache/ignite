/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.local;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Local deployment SPI start-stop test.
 */
@GridSpiTest(spi = LocalDeploymentSpi.class, group = "Deployment SPI")
public class GridLocalDeploymentSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<LocalDeploymentSpi> {
    // No-op.
}
