/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.roundrobin;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Tests correct start of {@link RoundRobinLoadBalancingSpi}.
 */
@SuppressWarnings({"JUnitTestCaseWithNoTests"})
@GridSpiTest(spi = RoundRobinLoadBalancingSpi.class, group = "LoadBalancing SPI")
public class GridRoundRobinLoadBalancingSpiStartStopSelfTest
    extends GridSpiStartStopAbstractTest<RoundRobinLoadBalancingSpi> {
    // No configs.
}
