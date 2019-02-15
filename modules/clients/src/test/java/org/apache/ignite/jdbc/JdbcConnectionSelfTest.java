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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Connection test.
 */
public class JdbcConnectionSelfTest extends GridCommonAbstractTest {
    /** Custom cache name. */
    private static final String CUSTOM_CACHE_NAME = "custom-cache";

    /** Custom REST TCP port. */
    private static final int CUSTOM_PORT = 11212;

    /** URL prefix. */
    private static final String URL_PREFIX = "jdbc:ignite://";

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME), cacheConfiguration(CUSTOM_CACHE_NAME));

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        if (!igniteInstanceName.endsWith("0"))
            clientCfg.setPort(CUSTOM_PORT);

        cfg.setConnectorConfiguration(clientCfg);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaults() throws Exception {
        String url = URL_PREFIX + HOST;

        assert DriverManager.getConnection(url) != null;
        assert DriverManager.getConnection(url + "/") != null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeId() throws Exception {
        String url = URL_PREFIX + HOST + "/?nodeId=" + grid(0).localNode().id();

        assert DriverManager.getConnection(url) != null;

        url = URL_PREFIX + HOST + "/" + CUSTOM_CACHE_NAME + "?nodeId=" + grid(0).localNode().id();

        assert DriverManager.getConnection(url) != null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomCache() throws Exception {
        String url = URL_PREFIX + HOST + "/" + CUSTOM_CACHE_NAME;

        assert DriverManager.getConnection(url) != null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomPort() throws Exception {
        String url = URL_PREFIX + HOST + ":" + CUSTOM_PORT;

        assert DriverManager.getConnection(url) != null;
        assert DriverManager.getConnection(url + "/") != null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomCacheNameAndPort() throws Exception {
        String url = URL_PREFIX + HOST + ":" + CUSTOM_PORT + "/" + CUSTOM_CACHE_NAME;

        assert DriverManager.getConnection(url) != null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongCache() throws Exception {
        final String url = URL_PREFIX + HOST + "/wrongCacheName";

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection(url);

                    return null;
                }
            },
            SQLException.class,
            "Client is invalid. Probably cache name is wrong."
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongPort() throws Exception {
        final String url = URL_PREFIX + HOST + ":33333";

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection(url);

                    return null;
                }
            },
            SQLException.class,
            "Failed to establish connection."
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClose() throws Exception {
        String url = URL_PREFIX + HOST;

        final Connection conn = DriverManager.getConnection(url);

        assert conn != null;
        assert !conn.isClosed();

        conn.close();

        assert conn.isClosed();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    conn.isValid(2);

                    return null;
                }
            },
            SQLException.class,
            "Connection is closed."
        );
    }
}
