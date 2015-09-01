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