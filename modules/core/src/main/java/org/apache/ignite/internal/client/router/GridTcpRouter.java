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

package org.apache.ignite.internal.client.router;

import java.util.UUID;

/**
 * TCP router interface.
 * <p>
 * Router allows remote clients to connect to grid without direct access to
 * the network where grid is running. It accepts requests on the same protocol
 * as grid rest server and establish required connection to grid nodes
 * to serve them.
 * <p>
 * Below is an example on how to start TCP router with non-default configuration.
 * <pre name="code" class="java">
 * GridTcpRouterConfiguration cfg =
 *     new GridTcpRouterConfiguration();
 *
 * cfg.setHost("router.appdomain.com");
 * cfg.setPort(11211);
 *
 * cfg.setServers(Arrays.asList(
 *     "node1.appdomain.com:11211",
 *     "node2.appdomain.com:11211"));
 *
 * GridRouterFactory.startTcpRouter(cfg);
 * </pre>
 * <p>
 * Note that clients should be specifically configured in order to use router.
 * Please refer to {@link org.apache.ignite.internal.client.GridClientConfiguration#getServers()} and
 * {@link org.apache.ignite.internal.client.GridClientConfiguration#getRouters()} documentation for more details.
 * <p>
 * Instances of this interface are managed through {@link GridRouterFactory}.
 *
 * @see GridTcpRouterConfiguration
 *
 */
public interface GridTcpRouter {
    /**
     * Returns router Id.
     * <p>
     * Unique router Ids are automatically generated on router startup.
     * They are used to control router's lifecycle via {@link GridRouterFactory}.
     *
     * @see GridRouterFactory#tcpRouter(UUID)
     * @see GridRouterFactory#stopTcpRouter(UUID)
     * @return Router Id.
     */
    public UUID id();

    /**
     * Returns configuration used to start router.
     *
     * @see GridRouterFactory#startTcpRouter(GridTcpRouterConfiguration)
     * @return Router configuration.
     */
    public GridTcpRouterConfiguration configuration();
}