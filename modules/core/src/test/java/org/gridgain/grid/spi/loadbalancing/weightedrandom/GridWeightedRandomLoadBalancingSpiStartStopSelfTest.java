/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.weightedrandom;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Wighted random load balancing SPI start-stop test.
 */
@SuppressWarnings({"JUnitTestCaseWithNoTests"})
@GridSpiTest(spi = WeightedRandomLoadBalancingSpi.class, group = "LoadBalancing SPI")
public class GridWeightedRandomLoadBalancingSpiStartStopSelfTest extends
    GridSpiStartStopAbstractTest<WeightedRandomLoadBalancingSpi> {
    // No configs.
}
