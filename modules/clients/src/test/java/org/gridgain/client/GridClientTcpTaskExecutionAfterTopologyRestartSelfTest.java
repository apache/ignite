/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Ensures
 */
public class GridClientTcpTaskExecutionAfterTopologyRestartSelfTest extends GridCommonAbstractTest {
    /** Port. */
    private static final int PORT = 11211;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        clientCfg.setRestTcpPort(PORT);

        cfg.setClientConnectionConfiguration(clientCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskAfterRestart() throws Exception {
        startGrids(1);

        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setProtocol(GridClientProtocol.TCP);
        cfg.setServers(Collections.singleton("127.0.0.1:" + PORT));

        GridClient cli = GridClientFactory.start(cfg);

        cli.compute().execute(GridClientTcpTask.class.getName(), Collections.singletonList("arg"));

        stopAllGrids();

        startGrid();

        cli.compute().execute(GridClientTcpTask.class.getName(), Collections.singletonList("arg"));
    }
}
