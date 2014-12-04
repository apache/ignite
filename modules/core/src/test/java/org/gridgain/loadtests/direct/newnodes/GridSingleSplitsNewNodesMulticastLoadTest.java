/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.newnodes;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.testframework.junits.common.*;

/**
 *
 */
@GridCommonTest(group = "Load Test")
public class GridSingleSplitsNewNodesMulticastLoadTest extends GridSingleSplitsNewNodesAbstractLoadTest {
    /** {@inheritDoc} */
    @Override protected GridDiscoverySpi getDiscoverySpi(IgniteConfiguration cfg) {
        GridDiscoverySpi discoSpi = cfg.getDiscoverySpi();

        assert discoSpi instanceof GridTcpDiscoverySpi: "Wrong default SPI implementation.";

        ((GridTcpDiscoverySpi)discoSpi).setHeartbeatFrequency(getHeartbeatFrequency());

        return discoSpi;
    }

    /** {@inheritDoc} */
    @Override protected int getHeartbeatFrequency() {
        return 3000;
    }
}
