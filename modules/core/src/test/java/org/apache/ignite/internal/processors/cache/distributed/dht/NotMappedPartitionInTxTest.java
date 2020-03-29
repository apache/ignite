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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public class NotMappedPartitionInTxTest extends GridCommonAbstractTest {
    /** Cache. */
    private static final String CACHE = "testCache";

    /** Cache 2. */
    public static final String CACHE2 = CACHE + 1;

    /** Test key. */
    private static final String TEST_KEY = "key";

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setCacheConfiguration(
                new CacheConfiguration(CACHE)
                    .setAtomicityMode(atomicityMode)
                    .setCacheMode(CacheMode.REPLICATED)
                    .setAffinity(new TestAffinity()),
                new CacheConfiguration(CACHE2)
                    .setAtomicityMode(atomicityMode));
    }

    /**
     *
     */
    @Test
    public void testOneServerTx() throws Exception {
        try {
            startGrid(0);

            final IgniteEx client = startClientGrid(1);

            checkNotMapped(client, OPTIMISTIC, REPEATABLE_READ);

            checkNotMapped(client, OPTIMISTIC, SERIALIZABLE);

            checkNotMapped(client, PESSIMISTIC, READ_COMMITTED);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @Test
    public void testOneServerMvcc() throws Exception {
        try {
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

            startGrid(0);

            final IgniteEx client = startClientGrid(1);

            checkNotMapped(client, PESSIMISTIC, REPEATABLE_READ);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @Test
    public void testFourServersTx() throws Exception {
        try {
            startGridsMultiThreaded(4);

            final IgniteEx client = startClientGrid(4);

            checkNotMapped(client, OPTIMISTIC, REPEATABLE_READ);

            checkNotMapped(client, OPTIMISTIC, SERIALIZABLE);

            checkNotMapped(client, PESSIMISTIC, READ_COMMITTED);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @Test
    public void testFourServersMvcc() throws Exception {
        try {
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

            startGridsMultiThreaded(4);

            final IgniteEx client = startClientGrid(4);

            checkNotMapped(client, PESSIMISTIC, REPEATABLE_READ);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param client Ignite client.
     */
    private void checkNotMapped(final IgniteEx client, final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) {
        String msg;

        if (atomicityMode == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            msg = "Failed to get primary node ";
        else
            msg = concurrency == PESSIMISTIC ? "Failed to lock keys (all partition nodes left the grid)" :
                "Failed to map keys to nodes (partition is not mapped to any node";

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() {
                IgniteCache cache2 = client.cache(CACHE2);
                IgniteCache cache1 = client.cache(CACHE).withKeepBinary();

                try (Transaction tx = client.transactions().txStart(concurrency, isolation)) {

                    Map<String, Integer> param = new TreeMap<>();
                    param.put(TEST_KEY + 1, 1);
                    param.put(TEST_KEY + 1, 3);
                    param.put(TEST_KEY, 3);

                    cache1.put(TEST_KEY, 3);

                    cache1.putAll(param);
                    cache2.putAll(param);

                    tx.commit();
                }

                return null;
            }
        }, ClusterTopologyServerNotFoundException.class, msg);
    }

    /** */
    private static class TestAffinity extends RendezvousAffinityFunction {
        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            if (TEST_KEY.equals(key))
                return 1;

            return super.partition(key);
        }

        /** {@inheritDoc} */
        @Override public List<ClusterNode> assignPartition(int part, List<ClusterNode> nodes, int backups,
            @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
            if (part == 1)
                return Collections.emptyList();

            return super.assignPartition(part, nodes, backups, neighborhoodCache);
        }
    }
}
