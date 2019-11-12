/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.common;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for cache creation and destruction from servers and clients: thin, thick, jdbc and rest.
 * Including simultaneous operations. Mainly within same cache group.
 */
@SuppressWarnings({"ThrowableNotThrown", "unchecked"})
public class ClientSizeCacheCreationDestructionTest extends GridCommonAbstractTest {
    /** **/
    private static final String CACHE_NAME = "CacheName";

    /** **/
    private static final String ANOTHER_CACHE_NAME = "AnotherCacheName";

    /** **/
    private static final String CLIENT_CACHE_NAME = "ClientCacheName";

    /** **/
    private static final String CACHE_GROUP_NAME = "CacheGroupName";

    /** **/
    protected Ignite srv;

    /** **/
    private Ignite thickClient;

    /** **/
    private IgniteClient thinClient;

    /** **/
    private Connection jdbcConn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConnectorConfiguration(new ConnectorConfiguration());

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        srv = startGrid("server");

        thickClient = startClientGrid(1);

        thinClient = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));

        jdbcConn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (thickClient != null)
            thickClient.close();

        if (thinClient != null)
            thinClient.close();

        if (jdbcConn != null)
            jdbcConn.close();

        stopAllGrids();
    }

    /**
     * Direct scenario:
     * <ol>
     *     <li>Start server node, create cache in cache group.</li>
     *     <li>Start client node and create cache in same cache group.</li>
     *     <li>Assert no exception, cache successfully created, value may be inserted into this cache.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerThenClientCacheCreation() throws Exception {
        createCache(srv, cacheConfig());

        createCache(thickClient, cacheConfig().setName(CLIENT_CACHE_NAME));

        IgniteCache cache = srv.cache(CLIENT_CACHE_NAME);

        cache.put(1L, "abc");

        assertEquals("abc", cache.get(1L));
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 4 different cache groups: each cache
     *     in corresponding cache group.</li>
     *     <li>Start <b>Thick</b> client node, create 1 new cache in each created cache group.</li>
     *     <li>Assert that 4 cache groups exist with 2 caches each.</li>
     *     <li>Try to insert and get some data from caches.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinFourCacheGroupsThickClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++)
            createCache(thickClient, cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CLIENT_CACHE_NAME + i));

        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CLIENT_CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache(CLIENT_CACHE_NAME + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i, srv.cache(CLIENT_CACHE_NAME + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 4 different cache groups: each cache
     *     in corresponding cache group.</li>
     *     <li>Start <b>Thin</b> client node, create 1 new cache in each created cache group.</li>
     *     <li>Assert that 4 cache groups exist with 2 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinFourCacheGroupsThinClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++) {
            createCache(thinClient, clientCacheConfig().setGroupName(CACHE_GROUP_NAME + i).
                setName(CLIENT_CACHE_NAME + i));
        }

        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CLIENT_CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache(CLIENT_CACHE_NAME + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i, srv.cache(CLIENT_CACHE_NAME + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 4 different cache groups: each cache
     *     in corresponding cache group.</li>
     *     <li>Start <b>Jdbc Thin</b> client node, create 1 new cache in each created cache group.</li>
     *     <li>Assert that 4 cache groups exist with 2 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinFourCacheGroupsJdbcThinClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++)
            createCache(jdbcConn, cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CLIENT_CACHE_NAME + i));

        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache("SQL_PUBLIC_" + CLIENT_CACHE_NAME.toUpperCase() + i).
                    getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache("SQL_PUBLIC_" + CLIENT_CACHE_NAME.toUpperCase() + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i,
                srv.cache("SQL_PUBLIC_" + CLIENT_CACHE_NAME.toUpperCase() + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 4 different cache groups: each cache
     *     in corresponding cache group.</li>
     *     <li>Start <b>Rest</b> client node, create 1 new cache in each created cache group.</li>
     *     <li>Assert that 4 cache groups exist with 2 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinFourCacheGroupsRestClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++)
            createCacheWithRestClient(cacheConfig().setGroupName(CACHE_GROUP_NAME + i).setName(CLIENT_CACHE_NAME + i));

        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + i,
                srv.cache(CLIENT_CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache(CLIENT_CACHE_NAME + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i, srv.cache(CLIENT_CACHE_NAME + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 2 different cache groups in server node (2+2).</li>
     *     <li>Start <b>Thick</b> client node, create 2 new caches in each created cache group.</li>
     *     <li>Assert that 2 cache groups exist with 4 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinTwoCacheGroupsThickClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++) {
            createCache(thickClient, cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).
                setName(CLIENT_CACHE_NAME + i));
        }

        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CLIENT_CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache(CLIENT_CACHE_NAME + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i, srv.cache(CLIENT_CACHE_NAME + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 2 different cache groups in server node (2+2).</li>
     *     <li>Start <b>Thin</b> client node, create 2 new caches in each created cache group.</li>
     *     <li>Assert that 2 cache groups exist with 4 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinTwoCacheGroupsThinClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++) {
            createCache(thinClient, clientCacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).
                setName(CLIENT_CACHE_NAME + i));
        }

        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CLIENT_CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache(CLIENT_CACHE_NAME + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i, srv.cache(CLIENT_CACHE_NAME + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 2 different cache groups in server node (2+2).</li>
     *     <li>Start <b>Jdbc Thin</b> client node, create 2 new caches in each created cache group.</li>
     *     <li>Assert that 2 cache groups exist with 4 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinTwoCacheGroupsJdbcThinClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++) {
            createCache(jdbcConn, cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).
                setName(CLIENT_CACHE_NAME + i));
        }
        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache("SQL_PUBLIC_" + CLIENT_CACHE_NAME.toUpperCase() + i).
                    getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache("SQL_PUBLIC_" + CLIENT_CACHE_NAME.toUpperCase() + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i,
                srv.cache("SQL_PUBLIC_" + CLIENT_CACHE_NAME.toUpperCase() + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches in 2 different cache groups in server node (2+2).</li>
     *     <li>Start <b>Rest</b> client node, create 2 new caches in each created cache group.</li>
     *     <li>Assert that 2 cache groups exist with 4 caches each.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithinTwoCacheGroupsRestClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).setName(CACHE_NAME + i));

        for (int i = 0; i < 4; i++) {
            createCacheWithRestClient(cacheConfig().setGroupName(CACHE_GROUP_NAME + (i % 2)).
                setName(CLIENT_CACHE_NAME + i));
        }
        // Assertions.
        assertEquals(8, srv.cacheNames().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            assertEquals(CACHE_GROUP_NAME + (i % 2),
                srv.cache(CLIENT_CACHE_NAME + i).getConfiguration(CacheConfiguration.class).getGroupName());

            srv.cache(CACHE_NAME + i).put(1, "abc_srv" + i);
            assertEquals("abc_srv" + i, srv.cache(CACHE_NAME + i).get(1));

            srv.cache(CLIENT_CACHE_NAME + i).put(1, "abc_cli" + i);
            assertEquals("abc_cli" + i, srv.cache(CLIENT_CACHE_NAME + i).get(1));
        }
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches without cache groups.</li>
     *     <li>Start <b>Thick</b> client node, try to create cache with
     *     cache group with a name == first cache name.</li>
     *     <li>{@code CacheException} expected with message:
     *     'Failed to start cache. Cache group name conflict with existing cache (change group name)'.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithCacheGroupNameEqualsFirstCacheNameThickClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfigWithoutCacheGroup().setName(CACHE_NAME + i));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCache(thickClient, cacheConfig().setGroupName(CACHE_NAME + 0).setName(CLIENT_CACHE_NAME));

                return null;
            },
            CacheException.class,
            "Failed to start cache. Cache group name conflict with existing cache (change group name)");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches without cache groups.</li>
     *     <li>Start <b>Thin</b> client node, try to create cache with cache group with a name == first cache name.</li>
     *     <li>{@code ClientServerError} expected with message:
     *     'Failed to start cache. Cache group name conflict with existing cache (change group name)'.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithCacheGroupNameEqualsFirstCacheNameThinClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfigWithoutCacheGroup().setName(CACHE_NAME + i));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCache(thinClient, clientCacheConfig().setGroupName(CACHE_NAME + 0).setName(CLIENT_CACHE_NAME));

                return null;
            },
            ClientServerError.class,
            "Failed to start cache. Cache group name conflict with existing cache (change group name)");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches without cache groups.</li>
     *     <li>Start <b>Jdbc Thin</b> client node, try to create cache
     *     with cache group with a name == first cache name.</li>
     *     <li>{@code SQLException} expected with message:
     *     'Failed to start cache. Cache group name conflict with existing cache (change group name)'.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithCacheGroupNameEqualsFirstCacheNameJdbcThinClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfigWithoutCacheGroup().setName(CACHE_NAME + i));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCache(jdbcConn, cacheConfig().setGroupName(CACHE_NAME + 0).setName(CLIENT_CACHE_NAME));

                return null;
            },
            SQLException.class,
            "Failed to start cache. Cache group name conflict with existing cache (change group name)");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches without cache groups.</li>
     *     <li>Start <b>Rest</b> client node, try to create cache with cache group with a name == first cache name.</li>
     *     <li>{@code Exception} expected.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithCacheGroupNameEqualsFirstCacheNameRestClient() throws Exception {
        for (int i = 0; i < 4; i++)
            createCache(srv, cacheConfigWithoutCacheGroup().setName(CACHE_NAME + i));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCacheWithRestClient(cacheConfig().setGroupName(CACHE_NAME + 0).setName(CLIENT_CACHE_NAME));
                return null;
            },
            AssertionError.class,
            "expected:<0> but was:<1>");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches with cache groups.</li>
     *     <li>Start <b>Thick</b> client node, try to create extra cache within same cache group but with different
     *     config.</li>
     *     <li>{@code CacheException} expected
     *     with message 'Backups mismatch for caches related to the same group'.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithDifferentConfigThickClient() throws Exception {
        createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME).setBackups(1));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCache(thickClient, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CLIENT_CACHE_NAME).
                    setBackups(2));

                return null;
            },
            CacheException.class,
            "Backups mismatch for caches related to the same group");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches with cache groups.</li>
     *     <li>Start <b>Thin</b> client node, try to create extra cache within same cache group but with different
     *     config.</li>
     *     <li>{@code ClientServerError} expected
     *     with message 'Backups mismatch for caches related to the same group'.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithDifferentConfigThinClient() throws Exception {
        createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME).setBackups(1));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCache(thinClient, clientCacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CLIENT_CACHE_NAME).
                    setBackups(2));

                return null;
            },
            ClientServerError.class,
            "Backups mismatch for caches related to the same group");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches with cache groups.</li>
     *     <li>Start <b>Jdbc Thin</b> client node, try to create extra cache within same cache group but with different
     *     config.</li>
     *     <li>{@code SQLException} expected with message 'Backups mismatch for caches related to the same group'.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithDifferentConfigJdbcThinClient() throws Exception {
        createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME).setBackups(1));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCache(jdbcConn, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CLIENT_CACHE_NAME).
                    setBackups(2));

                return null;
            },
            SQLException.class,
            "Backups mismatch for caches related to the same group");
    }

    /**
     * Few caches created in chain:
     * <ol>
     *     <li>Start server node, create 4 different caches with cache groups.</li>
     *     <li>Start <b>Rest</b> client node, try to create extra cache within same cache group but with different
     *     config.</li>
     *     <li>Exception is expected.</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testFewCachesCreatedInChainWithDifferentConfigRestClient() throws Exception {
        createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME).setBackups(1));

        GridTestUtils.assertThrows(
            null,
            () -> {
                createCacheWithRestClient(cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CLIENT_CACHE_NAME).
                    setBackups(2));

                return null;
            },
            AssertionError.class,
            "expected:<0> but was:<1>");
    }

    /**
     * Destroy caches:
     * <ol>
     *     <li>Start server node, create 2 caches in single cache group.</li>
     *     <li>Start <b>Thick</b> client and try to destroy 2 caches at the same time from client and from server.</li>
     *     <li>Assert that operation completed successfully, both caches are destroyed and cache group is no longer
     *     exists (for example create cache with same name as deleted cache group)</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyCachesThickClient() throws Exception {
        for (int i = 0; i < 2; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME + i));

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture srv = GridTestUtils.runAsync(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail(e.toString());
            }
            this.srv.destroyCache(CACHE_NAME + 0);
        });

        IgniteInternalFuture client = GridTestUtils.runAsync(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail(e.toString());
            }
            thickClient.destroyCache(CACHE_NAME + 1);
        });

        latch.countDown();

        srv.get();

        client.get();

        assertEquals(0, this.srv.cacheNames().size());

        this.srv.createCache(CACHE_GROUP_NAME);
    }

    /**
     * Destroy caches:
     * <ol>
     *     <li>Start server node, create 2 caches in single cache group.</li>
     *     <li>Start <b>Thin</b> client and try to destroy 2 caches at the same time from client and from server.</li>
     *     <li>Assert that operation completed successfully, both caches are destroyed and cache group is no longer
     *     exists (for example create cache with same name as deleted cache group)</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyCachesThinClient() throws Exception {
        for (int i = 0; i < 2; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME + i));

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture srv = GridTestUtils.runAsync(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail(e.toString());
            }
            this.srv.destroyCache(CACHE_NAME + 0);
        });

        IgniteInternalFuture client = GridTestUtils.runAsync(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail(e.toString());
            }
            thinClient.destroyCache(CACHE_NAME + 1);
        });

        latch.countDown();

        srv.get();

        client.get();

        assertEquals(0, this.srv.cacheNames().size());

        this.srv.createCache(CACHE_GROUP_NAME);
    }

    /**
     * Destroy caches:
     * <ol>
     *     <li>Start server node, create 2 caches in single cache group.</li>
     *     <li>Start <b>Rest</b> client and try to destroy 2 caches at the same time from client and from server.</li>
     *     <li>Assert that operation completed successfully, both caches are destroyed and cache group is no longer
     *     exists (for example create cache with same name as deleted cache group)</li>
     * </ol>
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyCachesRestClient() throws Exception {
        for (int i = 0; i < 2; i++)
            createCache(srv, cacheConfig().setGroupName(CACHE_GROUP_NAME).setName(CACHE_NAME + i));

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture srv = GridTestUtils.runAsync(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail(e.toString());
            }
            this.srv.destroyCache(CACHE_NAME + 0);
        });

        IgniteInternalFuture client = GridTestUtils.runAsync(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail(e.toString());
            }

            URLConnection conn = null;
            try {
                conn = new URL("http://localhost:8080/ignite?cmd=destcache&cacheName=" + CACHE_NAME + "1").
                    openConnection();
            }
            catch (IOException e) {
                fail(e.toString());
            }

            try {
                conn.connect();

                try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
                    ObjectMapper objMapper = new ObjectMapper();
                    Map<String, Object> myMap = objMapper.readValue(streamReader,
                        new TypeReference<Map<String, Object>>() {
                        });

                    log.info("Version command response is: " + myMap);

                    assertTrue(myMap.containsKey("response"));
                    assertEquals(0, myMap.get("successStatus"));
                }
            }
            catch (IOException e) {
                fail(e.toString());
            }

        });

        latch.countDown();

        srv.get();

        client.get();

        assertEquals(0, this.srv.cacheNames().size());

        this.srv.createCache(CACHE_GROUP_NAME);
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Thick</b> client.</li>
     *         <li>Create new cache with an existing cache group on server side.</li>
     *         <li>Destroy newly created cache through client.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b>
     *      Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnSrvDestroyOnThickClient() {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        srv.createCache(cacheConfig());

        thickClient.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Thin</b> client.</li>
     *         <li>Create new cache with an existing cache group on server side.</li>
     *         <li>Destroy newly created cache through client.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b>
     *      Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnSrvDestroyOnThinClient() {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        srv.createCache(cacheConfig());

        thinClient.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Rest</b> client.</li>
     *         <li>Create new cache with an existing cache group on server side.</li>
     *         <li>Destroy newly created cache through client.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b>
     *      Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnSrvDestroyOnRestClient() throws Exception {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        srv.createCache(cacheConfig());

        destroyCacheWithRestClient(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Thick</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through server node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b>
     *      Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnThickClientDestroyOnSrv() {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        thickClient.createCache(cacheConfig());

        srv.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Thin</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through server node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b>
     *      Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnThinClientSrvDestroyOnSrv() {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        thinClient.createCache(clientCacheConfig());

        srv.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>jdbc</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through server node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b> Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnJdbcClientDestroyOnSrv() throws Exception {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        createCache(jdbcConn, cacheConfig());

        srv.destroyCache("SQL_PUBLIC_" + CACHE_NAME.toUpperCase());

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Rest</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through server node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b> Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnRestClientDestroyOnSrv() throws Exception {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        createCacheWithRestClient(cacheConfig());

        srv.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Thick</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through some other, previously created, <b>Thin</b> client node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b> Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnThickClientDestroyThinClient() {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        thickClient.createCache(cacheConfig());

        thinClient.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Thin</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through some other, previously created, <b>Rest</b> client node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b> Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnThinClientSrvDestroyOnRestClient() throws Exception{
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        thinClient.createCache(clientCacheConfig());

        destroyCacheWithRestClient(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Jdbc Thin</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through some other, previously created, <b>Thin</b> client node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b> Only one cache, initially created within server node is expected.
     */
    @Test
    public void testCreateOnJdbcClientDestroyOnThinClient() throws Exception {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        createCache(jdbcConn, cacheConfig());

        thinClient.destroyCache("SQL_PUBLIC_" + CACHE_NAME.toUpperCase());

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create and destroy caches:
     * <p>
     *     <b>Prerequisites:</b>
     *     Start server node, create 1 cache in a single cache group.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>Start <b>Jdbc Thin</b> client.</li>
     *         <li>Create new cache with an existing cache group on client side.</li>
     *         <li>Destroy newly created cache through some other, previously created, <b>Thick</b> client node.</li>
     *     </ol>
     * <p>
     *      <b>Expected:</b> Only one cache, initially created within server node is expected.
     * @throws Exception If failed.
     */
    @Test
    public void testCreateOnRestClientDestroyOnThickClient() throws Exception {
        srv.createCache(cacheConfig().setName(ANOTHER_CACHE_NAME));

        createCacheWithRestClient(cacheConfig());

        thickClient.destroyCache(CACHE_NAME);

        assertEquals(1, srv.cacheNames().size());

        assertEquals(ANOTHER_CACHE_NAME, srv.cacheNames().iterator().next());
    }

    /**
     * Create cache with specified configuration through thin/thick client or jdbc thin.
     *
     * @param node Cluster node or jdbc connection.
     * @param cacheCfg Cache or ClientCache configuration
     * @throws SQLException If failed to create cache through Jdbc Thin connection.
     */
    private void createCache(AutoCloseable node, Serializable cacheCfg) throws SQLException {
        if (node instanceof IgniteClient)
            ((IgniteClient)node).createCache((ClientCacheConfiguration)cacheCfg);
        else if (node instanceof Ignite)
            ((Ignite)node).createCache((CacheConfiguration)cacheCfg);
        else if (node instanceof JdbcThinConnection) {
            CacheConfiguration jdbcCacheCfg = (CacheConfiguration)cacheCfg;

            srv.addCacheConfiguration(jdbcCacheCfg);

            try (Statement stmt = jdbcConn.createStatement()) {
                stmt.execute("CREATE TABLE " + jdbcCacheCfg.getName() +
                    " (id int, name varchar, primary key (id)) WITH \"template=" + jdbcCacheCfg.getName() + "\"");
            }
        }
        else
            fail(" Unexpected node/client type");
    }

    /**
     * Create cache with specified configuration through rest client.
     * @param cacheCfg Cache configuration.
     * @throws Exception If failed.
     */
    private void createCacheWithRestClient(CacheConfiguration cacheCfg) throws Exception {
        srv.addCacheConfiguration(cacheCfg);

        URLConnection conn = new URL("http://localhost:8080/ignite?cmd=getorcreate&cacheName=" +
            cacheCfg.getName() + "&templateName=" + cacheCfg.getName()).openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            ObjectMapper objMapper = new ObjectMapper();
            Map<String, Object> myMap = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            log.info("Version command response is: " + myMap);

            assertTrue(myMap.containsKey("response"));
            assertEquals(0, myMap.get("successStatus"));
        }
    }

    /**
     * Destroy cache from within rest client.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void destroyCacheWithRestClient(String cacheName) throws Exception {
        URLConnection conn = new URL("http://localhost:8080/ignite?cmd=destcache&cacheName=" + cacheName).
            openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            ObjectMapper objMapper = new ObjectMapper();
            Map<String, Object> myMap = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            log.info("Version command response is: " + myMap);

            assertTrue(myMap.containsKey("response"));
            assertEquals(0, myMap.get("successStatus"));
        }
    }

    /**
     * @return Default client cache configuration.
     */
    private ClientCacheConfiguration clientCacheConfig() {
        return new ClientCacheConfiguration().
            setGroupName(CACHE_GROUP_NAME).
            setName(CACHE_NAME).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setCacheMode(CacheMode.PARTITIONED);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfig() {
        return new CacheConfiguration().
            setGroupName(CACHE_GROUP_NAME).
            setName(CACHE_NAME).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setCacheMode(CacheMode.PARTITIONED);
    }

    /**
     * @return Default cache configuration without cache group.
     */
    private CacheConfiguration cacheConfigWithoutCacheGroup() {
        return new CacheConfiguration().
            setName(CACHE_NAME).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setCacheMode(CacheMode.PARTITIONED);
    }
}
