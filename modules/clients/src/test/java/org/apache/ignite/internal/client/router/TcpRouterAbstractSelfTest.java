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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.integration.ClientAbstractSelfTest;
import org.apache.ignite.internal.client.router.impl.GridTcpRouterImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.log4j.Log4JLogger;

/**
 * Abstract base class for http routing tests.
 */
public abstract class TcpRouterAbstractSelfTest extends ClientAbstractSelfTest {
    /** Port number to use by router. */
    private static final int ROUTER_PORT = BINARY_PORT + 1;

    /** TCP router instance. */
    private static GridTcpRouterImpl router;

    /** Send count. */
    private static long sndCnt;

    /** Receive count. */
    private static long rcvCnt;

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sndCnt = router.getSendCount();
        rcvCnt = router.getReceivedCount();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assert router.getSendCount() > sndCnt :
            "Failed to ensure network activity [currCnt=" + router.getSendCount() + ", oldCnt=" + sndCnt + ']';
        assert router.getReceivedCount() > rcvCnt:
            "Failed to ensure network activity [currCnt=" + router.getReceivedCount() + ", oldCnt=" + rcvCnt + ']';
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        router = new GridTcpRouterImpl(routerConfiguration());

        router.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        router.stop();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());
        cfg.setRouters(Collections.singleton(HOST + ":" + ROUTER_PORT));

        return cfg;
    }

    /**
     * @return Router configuration.
     * @throws IgniteCheckedException If failed.
     */
    public GridTcpRouterConfiguration routerConfiguration() throws IgniteCheckedException {
        GridTcpRouterConfiguration cfg = new GridTcpRouterConfiguration();

        cfg.setHost(HOST);
        cfg.setPort(ROUTER_PORT);
        cfg.setPortRange(0);
        cfg.setServers(Collections.singleton(HOST+":"+BINARY_PORT));
        cfg.setLogger(new Log4JLogger(ROUTER_LOG_CFG));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Override public void testConnectable() throws Exception {
        GridClient client = client();

        List<GridClientNode> nodes = client.compute().refreshTopology(false, false);

        assertFalse(F.first(nodes).connectable());
    }
}