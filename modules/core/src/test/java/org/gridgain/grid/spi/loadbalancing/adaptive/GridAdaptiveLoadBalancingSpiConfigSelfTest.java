/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.adaptive;

import org.gridgain.testframework.junits.spi.*;

/**
 *
 */
@GridSpiTest(spi = AdaptiveLoadBalancingSpi.class, group = "LoadBalancing SPI")
public class GridAdaptiveLoadBalancingSpiConfigSelfTest
    extends GridSpiAbstractConfigTest<AdaptiveLoadBalancingSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new AdaptiveLoadBalancingSpi(), "loadProbe", null);
    }
}
