/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.always;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Always-failover SPI start-stop test.
 */
@GridSpiTest(spi = AlwaysFailoverSpi.class, group = "Failover SPI")
public class GridAlwaysFailoverSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<FailoverSpi> {
    // No-op.
}
