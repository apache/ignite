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

package org.apache.ignite.spi.discovery.tcp;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests cases when node connects to cluster with different SSL configuration.
 * Exception with meaningful message should be thrown.
 */
public class TcpDiscoverySslSecuredUnsecuredTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(gridName.contains("client"));

        if (gridName.contains("ssl"))
            cfg.setSslContextFactory(GridTestUtils.sslFactory());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSecuredUnsecuredServerConnection() throws Exception {
        checkConnection("plain-server", "ssl-server");
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnsecuredSecuredServerConnection() throws Exception {
        checkConnection("ssl-server", "plain-server");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSecuredClientUnsecuredServerConnection() throws Exception {
        checkConnection("plain-server", "ssl-client");
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnsecuredClientSecuredServerConnection() throws Exception {
        checkConnection("ssl-server", "plain-client");
    }

    /**
     * @param name1 First grid name.
     * @param name2 Second grid name.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkConnection(final String name1, final String name2) throws Exception {
        startGrid(name1);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(name2);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }
}
