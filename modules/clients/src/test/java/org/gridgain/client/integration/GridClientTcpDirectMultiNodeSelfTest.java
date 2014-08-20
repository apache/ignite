/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;

import java.util.*;

/**
 *
 */
public class GridClientTcpDirectMultiNodeSelfTest extends GridClientAbstractMultiNodeSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        assert NODES_CNT > 3 : "Too few nodes to execute direct multinode test";

        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());

        Collection<String> srvs = new ArrayList<>(3);

        for (int i = 0; i < NODES_CNT / 2; i++)
            srvs.add(HOST + ':' + (REST_TCP_PORT_BASE + i));

        cfg.setRouters(srvs);

        return cfg;
    }
}
