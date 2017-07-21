/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.impl.connection;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;

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
        Collection<InetSocketAddress> routers, GridClientTopology top, Byte marshId, boolean routerClient)
        throws GridClientException {
        super(clientId, sslCtx, cfg, routers, top, marshId, routerClient);
    }

    /** {@inheritDoc} */
    @Override protected void init0() throws GridClientException {
        // No-op.
    }
}