/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.apache.ignite.*;

/**
 * Router NIO listener.
 */
class GridTcpRouterNioListenerOsImpl extends GridTcpRouterNioListenerAdapter {
    /**
     * @param log Logger.
     * @param client Client for grid access.
     */
    GridTcpRouterNioListenerOsImpl(IgniteLogger log, GridRouterClientImpl client) {
        super(log, client);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        // No-op.
    }
}
