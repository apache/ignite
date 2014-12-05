/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.never;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Never-failover SPI start-stop test.
 */
@GridSpiTest(spi = NeverFailoverSpi.class, group = "Failover SPI")
public class GridNeverFailoverSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<FailoverSpi> {
    // No-op.
}
