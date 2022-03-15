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

package org.apache.ignite.util;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ThinClientExpirePolicyTest extends GridCommonAbstractTest {
    /** */
    private static final int TIMEOUT_SEC = 20;

    /** */
    private static final int TOPOLOGY_UPDATE_DELAY_MILLS = 2000;

    /** */
    public static volatile boolean isReady = true;

    /** */
    private final Consumer<IgniteClient> clientConsumer = client -> {
        ClientCache<UUID, UUID> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        while (!Thread.currentThread().isInterrupted() && isReady) {
            try {
                UUID uuid = UUID.randomUUID();

                cache.put(uuid, uuid);
            }
            catch (ClientException e) {
                // No-op.
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc}
     * @return*/
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        long time = TimeUnit.SECONDS.toMillis(TIMEOUT_SEC);

        PlatformExpiryPolicyFactory plc = new PlatformExpiryPolicyFactory(time, time, time);

        CacheConfiguration<UUID, UUID> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setExpiryPolicyFactory(plc);
        cacheCfg.setBackups(2);

        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TcpCommunicationSpi() {
                @Override public void sendMessage(ClusterNode node, Message msg,
                    IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

                    if (((GridIoMessage)msg).message() instanceof GridDhtPartitionsFullMessage) {
                        try {
                            U.sleep(TOPOLOGY_UPDATE_DELAY_MILLS);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            // No-op.
                        }
                    }

                    super.sendMessage(node, msg, ackC);
                }
            })
            .setCacheConfiguration(cacheCfg)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /**
     * @throws Exception
     */
    @Test
    public void expirePolicyTest() throws Exception {
        IgniteEx igniteEx = startGrids(2);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = igniteEx.getOrCreateCache(DEFAULT_CACHE_NAME);

        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);

        TimeUnit.SECONDS.sleep(TIMEOUT_SEC);

        isReady = false;

        igniteEx.cluster().state(ClusterState.INACTIVE);

        TimeUnit.SECONDS.sleep(TIMEOUT_SEC);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        assertEquals(2, igniteEx.context().discovery().aliveServerNodes().size());
        assertEquals(ClusterState.ACTIVE, G.allGrids().get(0).cluster().state());
        assertEquals(ClusterState.ACTIVE, G.allGrids().get(1).cluster().state());

        U.sleep(TOPOLOGY_UPDATE_DELAY_MILLS * 2);

        assertEquals(0, cache.size());
    }

    /**
     * @return Thin client for specified user.
     */
    private Thread startIgniteClientDaemonThread(Consumer<IgniteClient> consumer) {
        Thread thread = new Thread(() -> {
            try (IgniteClient igniteClient = Ignition.startClient(
                new ClientConfiguration().setAddresses(Config.SERVER))) {
                consumer.accept(igniteClient);
            }
        });

        thread.setDaemon(true);

        thread.start();

        return thread;
    }
}
