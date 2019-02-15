/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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