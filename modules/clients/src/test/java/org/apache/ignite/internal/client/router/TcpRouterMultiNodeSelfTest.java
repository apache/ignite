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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.integration.ClientAbstractMultiNodeSelfTest;
import org.apache.ignite.internal.client.router.impl.GridTcpRouterImpl;
import org.apache.ignite.logger.log4j.Log4JLogger;

import static org.apache.ignite.internal.client.integration.ClientAbstractSelfTest.ROUTER_LOG_CFG;

/**
 *
 */
public class TcpRouterMultiNodeSelfTest extends ClientAbstractMultiNodeSelfTest {
    /** Number of routers to start in this test. */
    private static final int ROUTERS_CNT = 5;

    /** Where to start routers' port numeration. */
    private static final int ROUTER_TCP_PORT_BASE = REST_TCP_PORT_BASE + NODES_CNT;

    /** Collection of routers. */
    private static Collection<GridTcpRouterImpl> routers = new ArrayList<>(ROUTERS_CNT);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < ROUTERS_CNT; i++)
            routers.add(new GridTcpRouterImpl(routerConfiguration(i++)));

        for (GridTcpRouterImpl r : routers)
            r.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        info("Stopping routers...");

        for (GridTcpRouterImpl r : routers)
            r.stop();

        info("Routers stopped.");

        routers.clear();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /**
     * @param i Number of router. Used to avoid configuration conflicts.
     * @return Router configuration.
     * @throws IgniteCheckedException If failed.
     */
    private GridTcpRouterConfiguration routerConfiguration(int i) throws IgniteCheckedException {
        GridTcpRouterConfiguration cfg = new GridTcpRouterConfiguration();

        cfg.setHost(HOST);
        cfg.setPort(ROUTER_TCP_PORT_BASE + i);
        cfg.setPortRange(0);
        cfg.setServers(Collections.singleton(HOST + ":" + REST_TCP_PORT_BASE));
        cfg.setLogger(new Log4JLogger(ROUTER_LOG_CFG));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());

        Collection<String> rtrs = new ArrayList<>(ROUTERS_CNT);

        for (int i = 0; i < ROUTERS_CNT; i++)
            rtrs.add(HOST + ':' + (ROUTER_TCP_PORT_BASE + i));

        cfg.setRouters(rtrs);

        return cfg;
    }
}