/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Random start stop test for {@link TcpDiscoverySpi}.
 */
@GridSpiTest(spi = TcpDiscoverySpi.class, group = "Discovery SPI")
public class GridTcpDiscoverySpiRandomStartStopTest extends
    GridAbstractDiscoveryRandomStartStopTest<TcpDiscoverySpi> {
    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int getMaxInterval() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override protected void spiConfigure(TcpDiscoverySpi spi) throws Exception {
        super.spiConfigure(spi);

        spi.setIpFinder(ipFinder);
    }
}
