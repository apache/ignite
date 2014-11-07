/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl.connection;

import org.gridgain.client.*;

import javax.net.ssl.*;
import java.net.*;
import java.util.*;

/**
 * Open source version of connection manager.
 */
public class GridClientConnectionManagerOsImpl extends GridClientConnectionManagerAdapter {
    /**
     * @param clientId Client ID.
     * @param sslCtx SSL context to enable secured connection or {@code null} to use unsecured one.
     * @param cfg Client configuration.
     * @param routers Routers or empty collection to use endpoints from topology info.
     * @param top Topology.
     * @throws GridClientException In case of error.
     */
    public GridClientConnectionManagerOsImpl(UUID clientId, SSLContext sslCtx, GridClientConfiguration cfg,
        Collection<InetSocketAddress> routers, GridClientTopology top, Byte marshId) throws GridClientException {
        super(clientId, sslCtx, cfg, routers, top, marshId);
    }

    /** {@inheritDoc} */
    @Override protected void init0() throws GridClientException {
        // No-op.
    }
}
