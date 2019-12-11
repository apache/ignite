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

package org.apache.ignite.jdbc.thin;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * This test emulates the connecting of JDBC thin client to Apache Ignite cluster in some corner cases.
 * For instance, connecting to an inactive cluster, connecting to a specific node when the initial exchange
 * is not started yet etc.
 */
public class JdbcThinConnectionToRestartedNodeSelfTest extends JdbcThinAbstractSelfTest {
    /** Default timeout that is used for SQL operations in millis. */
    private static final long TIMEOUT = 5_000;

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Custom port. */
    private static final int CUSTOM_JDBC_PORT = 12001;

    /** Table name. */
    private static final String TABLE_NAME = "DummyValue";

    /** SQL query. */
    private static final String SQL = "select * from " + TABLE_NAME + " where id > 2";

    /** Flag indicates the custom port should be used. */
    private boolean useCustomPort;

    /** Flag indicates the cache/table should be statically configured. */
    private boolean useStaticConfiguration = true;

    /** Latch allows to block exchange thread. @see Test*/
    private static CountDownLatch START_EXCHANGE = new CountDownLatch(1);

    /** Latch allows continuing regular work of the worker. */
    private static CountDownLatch FINISH_EXCHANGE = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        ClientConnectorConfiguration cliConf = new ClientConnectorConfiguration();

        if (useCustomPort)
            cliConf.setPort(CUSTOM_JDBC_PORT);

        cfg.setClientConnectorConfiguration(cliConf);

        if (useStaticConfiguration) {
            CacheConfiguration<?, ?> cacheCfg = defaultCacheConfiguration();
            cacheCfg.setSqlSchema("PUBLIC");
            cacheCfg.setBackups(1);
            cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 32));

            QueryEntity e = new QueryEntity();
            e.setKeyType(Integer.class.getName());
            e.setValueType(TABLE_NAME);
            e.setKeyFieldName("id");
            e.addQueryField("id", Integer.class.getName(), null);
            e.addQueryField("val", String.class.getName(), null);

            cacheCfg.setQueryEntities(Collections.singletonList(e));

            cfg.setCacheConfiguration(cacheCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.beforeTest();

        START_EXCHANGE = new CountDownLatch(1);
        FINISH_EXCHANGE = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        START_EXCHANGE.countDown();
        FINISH_EXCHANGE.countDown();

        JdbcThinConnectionToRestartedNodeTestPluginProvider.enable(false);

        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests jdbc connection to inactive node.
     *
     * @throws Exception If failed.
     */
    public void testJdbcConnectionToInactiveCluster() throws Exception {
        startGrid(0);

        GridTestUtils.assertThrows(
            log, () -> {
                try (Connection con = DriverManager.getConnection(URL)) {
                    try (Statement stmt = con.createStatement()) {
                        ResultSet rs = stmt.executeQuery(SQL);

                        return rs.next() ? rs.getString("val") : "";
                    }
                }
            },
            SQLException.class, "Can not perform the operation because the cluster is inactive.");
    }

    /**
     *
     * @throws Exception If failed.
     */
    public void testJdbcConnectionToRestartedNode() throws Exception {
        Ignite crd = startGrid(0);

        crd.cluster().active(true);

        createTableAndFillValues();

        stopGrid(0);

        // Let's hang exchange thread when the node is started in order to guarantee that jdbc request will be served
        // when the initial exchange has not started yet.
        checkJdbcConnectionToRestartedNode(URL, START_EXCHANGE, FINISH_EXCHANGE);
    }

    /**
     * Tests jdbc connection to the restarted node that is part of baseline topology. The table is statically configured.
     *
     * @throws Exception If failed.
     */
    public void testJdbcConnectionToRestartedNodeInActiveClusterStaticConfig() throws Exception {
        connectToRestartedNodeInActiveCluster();
    }

    /**
     * Tests jdbc connection to the restarted node that is part of baseline topology. The table is created dynamically.
     *
     * @throws Exception If failed.
     */
    public void testJdbcConnectionToRestartedNodeInActiveClusterDynamicConfig() throws Exception {
        useStaticConfiguration = false;

        connectToRestartedNodeInActiveCluster();
    }

    /**
     * Tests jdbc connection to the restarted node that is part of baseline topology.
     *
     * @throws Exception If failed.
     */
    private void connectToRestartedNodeInActiveCluster() throws Exception {
        Ignite crd = startGrid(0);

        useCustomPort = true;

        startGrid(1);

        crd.cluster().active(true);

        createTableAndFillValues();

        stopGrid(1);

        awaitPartitionMapExchange();

        // Let's hang exchange thread when the node is started in order to guarantee that jdbc request will be served
        // when the initial exchange has not started yet. It is important to note that the node will be started
        // in the active mode. It means the method GridComponent.onKernalStart will be called with {@code active}
        // equals to {@code true}.
        checkJdbcConnectionToRestartedNode(URL + ":" + CUSTOM_JDBC_PORT, START_EXCHANGE, FINISH_EXCHANGE);
    }

    /**
     * Tests jdbc connection to a restarted node.
     *
     * @param connUrl     Connection URL to be used.
     * @param startLatch  Latch that allows to wait for the first exchange task.
     * @param finishLatch Latch that allows to continue the regular work on the exchange thread.
     * @throws Exception If failed.
     */
    private void checkJdbcConnectionToRestartedNode(
        String connUrl,
        CountDownLatch startLatch,
        CountDownLatch finishLatch
    ) throws Exception {
        try {
            JdbcThinConnectionToRestartedNodeTestPluginProvider.enable(true);

            GridTestUtils.runAsync(() -> startGrid(1));

            startLatch.await();

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                try (Connection con = DriverManager.getConnection(connUrl)) {
                    try (Statement stmt = con.createStatement()) {
                        ResultSet rs = stmt.executeQuery(SQL);

                        return rs.next() ? rs.getString("val") : "";
                    }
                }
            });

            // The first attempt to get a result should lead to IgniteFutureTimeoutCheckedException
            // because the server node is waiting for the initial exchange.
            GridTestUtils.assertThrows(
                log,
                () -> fut.get(TIMEOUT, TimeUnit.MILLISECONDS), IgniteFutureTimeoutCheckedException.class,
                null);

            finishLatch.countDown();

            // After releasing the exchange thread the second attempt should get a result without any exceptions.
            fut.get(TIMEOUT, TimeUnit.MILLISECONDS);
        }
        finally {
            JdbcThinConnectionToRestartedNodeTestPluginProvider.enable(false);
        }
    }

    /**
     * Creates a new table if it does not exist and fills initial data into the table.
     */
    private void createTableAndFillValues() {
        CacheConfiguration cacheCfg = new CacheConfiguration<>("DUMMY_CACHE").setSqlSchema("PUBLIC");

        IgniteCache cache = grid(0).getOrCreateCache(cacheCfg);

        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
            + " (id INT PRIMARY KEY, val VARCHAR)")).getAll();

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO " + TABLE_NAME + " (id, val) VALUES (?, ?)");

        for (int i = 0; i < 3; ++i)
            cache.query(qry.setArgs(i, "val-" + i)).getAll();
    }

    /**
     * Test plugin provider that allows injecting a custom exchange task into the exchange worker
     * in order to block the worker before the first exchange happens.
     */
    public static class JdbcThinConnectionToRestartedNodeTestPluginProvider implements PluginProvider, IgnitePlugin {
        /** Flag indicates that this plugin provider is enabled. */
        private static volatile boolean enabled;

        /**
         * Enables this plugin provider.
         *
         * @param enable {@code true} if plugin provider should be enabled.
         */
        public static void enable(boolean enable) {
            enabled = enable;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "JdbcThinConnectionToRestartedNodeTestPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public String version() {
            return "1.0";
        }

        /** {@inheritDoc} */
        @Override public String copyright() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgnitePlugin plugin() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(
            PluginContext ctx,
            ExtensionRegistry registry
        ) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public Object createComponent(PluginContext ctx, Class cls) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
            if (enabled) {
                LinkedBlockingDeque<CachePartitionExchangeWorkerTask> queue = U.field(
                    (Object)U.field(
                        ((IgniteEx)ctx.grid()).context().cache().context().exchange(), "exchWorker"),
                    "futQ");

                queue.offer(
                    new CacheStatisticsModeChangeTask(new CustomTestMessage(START_EXCHANGE, FINISH_EXCHANGE)));
            }
        }


        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStop(boolean cancel) {
        }

        /** {@inheritDoc} */
        @Override public Serializable provideDiscoveryData(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        }

        /** {@inheritDoc} */
        @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        }
    }

    /**
     * Discovery message that is by JdbcThinConnectionToRestartedNodeTestPluginProvider to block the exchange worker.
     */
    public static class CustomTestMessage extends CacheStatisticsModeChangeMessage {
        /** Latch allows to block exchange thread. */
        private final CountDownLatch startLatch;

        /** Latch allows continuing regular work of the worker. */
        private final CountDownLatch finishLatch;

        /**
         * Creates a new instance of custom message.
         *
         * @param startLatch Latch is used to block exchange thread.
         * @param finishLatch Latch is used continuing regular work of the worker.
         */
        public CustomTestMessage(CountDownLatch startLatch, CountDownLatch finishLatch) {
            super(UUID.randomUUID(), Collections.EMPTY_SET, true);

            this.startLatch = startLatch;
            this.finishLatch = finishLatch;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> caches() {
            startLatch.countDown();

            try {
                finishLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException("The finish latch was unexpectedly interrupted.", e);
            }

            return super.caches();
        }
    }
}
