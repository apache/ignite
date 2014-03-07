/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast;

import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.testframework.*;

/**
 * GridTcpDiscoveryMulticastIpFinder test.
 */
public class GridTcpDiscoveryMulticastIpFinderSelfTest
    extends GridTcpDiscoveryIpFinderAbstractSelfTest<GridTcpDiscoveryMulticastIpFinder> {
    /**
     * @throws Exception In case of error.
     */
    public GridTcpDiscoveryMulticastIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryMulticastIpFinder ipFinder() throws Exception {
        GridTcpDiscoveryMulticastIpFinder ipFinder = new GridTcpDiscoveryMulticastIpFinder();

        ipFinder.setMulticastGroup(GridTestUtils.getNextMulticastGroup(getClass()));
        ipFinder.setMulticastPort(GridTestUtils.getNextMulticastPort(getClass()));

        return ipFinder;
    }
}
