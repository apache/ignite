/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.jdbc;

import com.mchange.v2.c3p0.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;

/**
 * JDBC IP finder self test.
 */
public class GridTcpDiscoveryJdbcIpFinderSelfTest extends
    GridTcpDiscoveryIpFinderAbstractSelfTest<GridTcpDiscoveryJdbcIpFinder> {
    /** */
    private ComboPooledDataSource dataSrc;

    /**
     * Constructor.
     *
     * @throws Exception If failed.
     */
    public GridTcpDiscoveryJdbcIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryJdbcIpFinder ipFinder() throws Exception {
        GridTcpDiscoveryJdbcIpFinder finder = new GridTcpDiscoveryJdbcIpFinder();

        assert finder.isShared() : "IP finder should be shared by default.";

        dataSrc = new ComboPooledDataSource();
        dataSrc.setDriverClass("org.h2.Driver");
        dataSrc.setJdbcUrl("jdbc:h2:mem");

        finder.setDataSource(dataSrc);

        return finder;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        dataSrc.close();
    }
}
