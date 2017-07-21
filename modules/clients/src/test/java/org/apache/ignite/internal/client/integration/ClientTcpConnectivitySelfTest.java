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

package org.apache.ignite.internal.client.integration;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.util.typedef.G;
import org.jetbrains.annotations.Nullable;

/**
 * Tests the REST client-server TCP connectivity with various configurations.
 */
public class ClientTcpConnectivitySelfTest extends ClientAbstractConnectivitySelfTest {
    /** {@inheritDoc} */
    @Override protected Ignite startRestNode(String name, @Nullable String addr, @Nullable Integer port)
        throws Exception {
        IgniteConfiguration cfg = getConfiguration(name);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        if (addr != null)
            clientCfg.setHost(addr);

        if (port != null)
            clientCfg.setPort(port);

        cfg.setConnectorConfiguration(clientCfg);

        return G.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected int defaultRestPort() {
        return IgniteConfiguration.DFLT_TCP_PORT;
    }

    /** {@inheritDoc} */
    @Override protected String restAddressAttributeName() {
        return IgniteNodeAttributes.ATTR_REST_TCP_ADDRS;
    }

    /** {@inheritDoc} */
    @Override protected String restHostNameAttributeName() {
        return IgniteNodeAttributes.ATTR_REST_TCP_HOST_NAMES;
    }

    /** {@inheritDoc} */
    @Override protected String restPortAttributeName() {
        return IgniteNodeAttributes.ATTR_REST_TCP_PORT;
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }
}