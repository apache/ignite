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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Abstract thin client partition awareness test.
 */
@SuppressWarnings("rawtypes")
public abstract class ThinClientAbstractPartitionAwarenessTest extends GridCommonAbstractTest {
    /** Wait timeout. */
    protected static final long WAIT_TIMEOUT = 5_000L;

    /** Replicated cache name. */
    protected static final String REPL_CACHE_NAME = "replicated_cache";

    /** Partitioned cache name. */
    protected static final String PART_CACHE_NAME = "partitioned_cache";

    /** Partitioned with custom affinity cache name. */
    protected static final String PART_CUSTOM_AFFINITY_CACHE_NAME = "partitioned_custom_affinity_cache";

    /** Name of a partitioned cache with 0 backups. */
    protected static final String PART_CACHE_0_BACKUPS_NAME = "partitioned_0_backup_cache";

    /** Name of a partitioned cache with 1 backups. */
    protected static final String PART_CACHE_1_BACKUPS_NAME = "partitioned_1_backup_cache";

    /** Name of a partitioned cache with 3 backups. */
    protected static final String PART_CACHE_3_BACKUPS_NAME = "partitioned_3_backup_cache";

    /** Keys count. */
    protected static final int KEY_CNT = 30;

    /** Max cluster size. */
    protected static final int MAX_CLUSTER_SIZE = 4;

    /** Channels. */
    protected final TestTcpClientChannel[] channels = new TestTcpClientChannel[MAX_CLUSTER_SIZE];

    /** Operations queue. */
    protected final Queue<T2<TestTcpClientChannel, ClientOperation>> opsQueue = new ConcurrentLinkedQueue<>();

    /** Client instance. */
    protected IgniteClient client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg0 = new CacheConfiguration()
            .setName(REPL_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration ccfg1 = new CacheConfiguration()
            .setName(PART_CUSTOM_AFFINITY_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new CustomAffinityFunction());

        CacheConfiguration ccfg2 = new CacheConfiguration()
            .setName(PART_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setKeyConfiguration(
                new CacheKeyConfiguration(TestNotAnnotatedAffinityKey.class.getName(), "affinityKey"),
                new CacheKeyConfiguration(TestAnnotatedAffinityKey.class));

        CacheConfiguration ccfg3 = new CacheConfiguration()
                .setName(PART_CACHE_0_BACKUPS_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0);

        CacheConfiguration ccfg4 = new CacheConfiguration()
                .setName(PART_CACHE_1_BACKUPS_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1);

        CacheConfiguration ccfg5 = new CacheConfiguration()
                .setName(PART_CACHE_3_BACKUPS_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(3);

        return cfg.setCacheConfiguration(ccfg0, ccfg1, ccfg2, ccfg3, ccfg4, ccfg5);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        opsQueue.clear();

        U.closeQuiet(client);

        client = null;
    }

    /**
     * Checks that operation goes through specified channel.
     */
    protected void assertOpOnChannel(@Nullable TestTcpClientChannel expCh, ClientOperation expOp) {
        T2<TestTcpClientChannel, ClientOperation> nextChOp = opsQueue.poll();

        assertNotNull("Unexpected (null) next operation [expCh=" + expCh + ", expOp=" + expOp + ']', nextChOp);

        assertEquals("Unexpected operation on channel [expCh=" + expCh + ", expOp=" + expOp +
                ", nextOpCh=" + nextChOp + ']', expOp, nextChOp.get2());

        if (expCh != null) {
            assertEquals("Unexpected channel for operation [expCh=" + expCh + ", expOp=" + expOp +
                ", nextOpCh=" + nextChOp + ']', expCh, nextChOp.get1());
        }
    }

    /**
     * Calculates affinity channel for cache and key.
     */
    protected TestTcpClientChannel affinityChannel(Object key, IgniteInternalCache<Object, Object> cache) {
        Collection<ClusterNode> nodes = cache.affinity().mapKeyToPrimaryAndBackups(key);

        UUID nodeId = nodes.iterator().next().id();

        return nodeChannel(nodeId);
    }

    /**
     * Calculates channel for node.
     */
    protected TestTcpClientChannel nodeChannel(UUID nodeId) {
        for (int i = 0; i < channels.length; i++) {
            if (channels[i] != null && nodeId.equals(channels[i].serverNodeId()))
                return channels[i];
        }

        return null;
    }

    /**
     * @param nodeIdxs Node indexes to connect to.
     */
    protected ClientConfiguration getClientConfiguration(int... nodeIdxs) {
        String addrs[] = Arrays.stream(nodeIdxs).mapToObj(nodeIdx -> "127.0.0.1:" + (DFLT_PORT + nodeIdx))
            .toArray(String[]::new);

        return new ClientConfiguration().setAddresses(addrs).setPartitionAwarenessEnabled(true);
    }

    /**
     * @param clientCfg Client configuration.
     * @param chIdxs Channels to wait for initialization.
     */
    protected void initClient(ClientConfiguration clientCfg, int... chIdxs) throws IgniteInterruptedCheckedException {
        client = new TcpIgniteClient((cfg, hnd) -> {
            try {
                log.info("Establishing connection to " + cfg.getAddresses());

                TcpClientChannel ch = new TestTcpClientChannel(cfg, hnd);

                log.info("Channel initialized: " + ch);

                return ch;
            }
            catch (Exception e) {
                log.warning("Failed to initialize channel: " + e.getMessage());

                throw e;
            }
        }, clientCfg);

        awaitChannelsInit(chIdxs);

        opsQueue.clear();
    }

    /**
     * Trigger client to detect topology change.
     */
    protected void detectTopologyChange() {
        opsQueue.clear();

        // Send non-affinity request to detect topology change.
        client.getOrCreateCache(REPL_CACHE_NAME);

        assertOpOnChannel(null, ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME);
    }

    /**
     * @param chIdxs Channel idxs.
     */
    protected void awaitChannelsInit(int... chIdxs) throws IgniteInterruptedCheckedException {
        // Wait until all channels initialized.
        for (int ch : chIdxs) {
            assertTrue("Failed to wait for channel[" + ch + "] init",
                GridTestUtils.waitForCondition(() -> channels[ch] != null, WAIT_TIMEOUT));
        }
    }

    /**
     *
     */
    private static class CustomAffinityFunction extends RendezvousAffinityFunction {
        // No-op.
    }

    /**
     * Test class without affinity key.
     */
    protected static class TestComplexKey {
        /** */
        int firstField;

        /** Another field. */
        int secondField;

        /** */
        public TestComplexKey(int firstField, int secondField) {
            this.firstField = firstField;
            this.secondField = secondField;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return firstField + secondField;
        }
    }

    /**
     * Test class with annotated affinity key.
     */
    protected static class TestAnnotatedAffinityKey {
        /** */
        @AffinityKeyMapped
        int affinityKey;

        /** */
        int anotherField;

        /** */
        public TestAnnotatedAffinityKey(int affinityKey, int anotherField) {
            this.affinityKey = affinityKey;
            this.anotherField = anotherField;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return affinityKey + anotherField;
        }
    }

    /**
     * Test class with affinity key without annotation.
     */
    protected static class TestNotAnnotatedAffinityKey {
        /** */
        TestComplexKey affinityKey;

        /** */
        int anotherField;

        /** */
        public TestNotAnnotatedAffinityKey(TestComplexKey affinityKey, int anotherField) {
            this.affinityKey = affinityKey;
            this.anotherField = anotherField;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return affinityKey.hashCode() + anotherField;
        }
    }

    /**
     * Test TCP client channel.
     */
    protected class TestTcpClientChannel extends TcpClientChannel {
        /** Channel configuration. */
        private final ClientChannelConfiguration cfg;

        /** Channel is closed. */
        private volatile boolean closed;

        /**
         * @param cfg Config.
         */
        public TestTcpClientChannel(ClientChannelConfiguration cfg, ClientConnectionMultiplexer hnd) {
            super(cfg, hnd);

            this.cfg = cfg;

            int chIdx = F.first(cfg.getAddresses()).getPort() - DFLT_PORT;

            channels[chIdx] = this;

            addTopologyChangeListener(ch -> log.info("Topology change detected [ch=" + ch + ", topVer=" +
                ch.serverTopologyVersion() + ']'));
        }

        /** {@inheritDoc} */
        @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader) throws ClientException {
            T res = super.service(op, payloadWriter, payloadReader);

            // Store all operations except some implicit system ops in queue to check later.
            if (op != ClientOperation.REGISTER_BINARY_TYPE_NAME
                && op != ClientOperation.PUT_BINARY_TYPE
                && op != ClientOperation.CLUSTER_GROUP_GET_NODE_ENDPOINTS
            )
                opsQueue.offer(new T2<>(this, op));

            return res;
        }

        /** {@inheritDoc} */
        @Override public <T> CompletableFuture<T> serviceAsync(
                ClientOperation op,
                Consumer<PayloadOutputChannel> payloadWriter,
                Function<PayloadInputChannel, T> payloadReader)
                throws ClientException {
            // Store all operations except some implicit system ops in queue to check later.
            if (op != ClientOperation.REGISTER_BINARY_TYPE_NAME
                && op != ClientOperation.PUT_BINARY_TYPE
                && op != ClientOperation.CLUSTER_GROUP_GET_NODE_ENDPOINTS
            )
                opsQueue.offer(new T2<>(this, op));

            return super.serviceAsync(op, payloadWriter, payloadReader);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            super.close();

            closed = true;
        }

        /**
         * Channel is closed.
         */
        public boolean isClosed() {
            return closed;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return cfg.getAddresses().toString();
        }
    }
}
