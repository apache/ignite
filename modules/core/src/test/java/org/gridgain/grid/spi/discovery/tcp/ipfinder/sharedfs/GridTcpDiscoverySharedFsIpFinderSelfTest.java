/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.sharedfs;

import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;

import java.io.*;
import java.util.*;

/**
 * GridTcpDiscoverySharedFsIpFinder test.
 */
public class GridTcpDiscoverySharedFsIpFinderSelfTest
    extends GridTcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoverySharedFsIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public GridTcpDiscoverySharedFsIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoverySharedFsIpFinder ipFinder() {
        TcpDiscoverySharedFsIpFinder finder = new TcpDiscoverySharedFsIpFinder();

        assert finder.isShared() : "Ip finder should be shared by default.";

        File tmpFile = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        assert !tmpFile.exists();

        if (!tmpFile.mkdir())
            assert false;

        finder.setPath(tmpFile.getAbsolutePath());

        return finder;
    }
}
