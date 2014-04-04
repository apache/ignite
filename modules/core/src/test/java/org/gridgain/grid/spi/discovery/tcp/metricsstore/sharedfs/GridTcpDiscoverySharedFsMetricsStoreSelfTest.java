/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.sharedfs;

import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;

import java.io.*;
import java.util.*;

/**
 * GridTcpDiscoverySharedFsMetricsStore test.
 */
public class GridTcpDiscoverySharedFsMetricsStoreSelfTest
    extends GridTcpDiscoveryMetricsStoreAbstractSelfTest<GridTcpDiscoverySharedFsMetricsStore> {
    /**
     * Constructor.
     *
     * @throws Exception If failed.
     */
    public GridTcpDiscoverySharedFsMetricsStoreSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoverySharedFsMetricsStore metricsStore() {
        GridTcpDiscoverySharedFsMetricsStore store = new GridTcpDiscoverySharedFsMetricsStore();

        File tmpFile = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        assert !tmpFile.exists();

        if (!tmpFile.mkdir())
            assert false;

        store.setPath(tmpFile.getAbsolutePath());

        return store;
    }
}
