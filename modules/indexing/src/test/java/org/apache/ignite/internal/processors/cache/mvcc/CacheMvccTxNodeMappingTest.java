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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test checks that transactions started on nodes collect all nodes participating in distributed transaction.
 */
public class CacheMvccTxNodeMappingTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        throw new RuntimeException("Is not supposed to be used");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAllTxNodesAreTrackedCli() throws Exception {
        checkAllTxNodesAreTracked(false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAllTxNodesAreTrackedSrv() throws Exception {
        checkAllTxNodesAreTracked(true);
    }

    /**
     * @param nearSrv {@code true} specifies that node initiated tx is server, otherwise it is client.
     */
    private void checkAllTxNodesAreTracked(boolean nearSrv) throws Exception {
        int srvCnt = 4;

        startGridsMultiThreaded(srvCnt);

        IgniteEx ign;

        if (nearSrv)
            ign = grid(0);
        else {
            client = true;

            ign = startGrid(srvCnt);
        }

        IgniteCache<Object, Object> cache = ign.createCache(basicCcfg().setBackups(2));

        Affinity<Object> aff = ign.affinity(cache.getName());

        Integer k1 = null, k2 = null;

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(0).localNode(), i)
                && aff.isBackup(grid(1).localNode(), i)
                && aff.isBackup(grid(2).localNode(), i)) {
                k1 = i;
                break;
            }
        }

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(1).localNode(), i)
                && aff.isBackup(grid(0).localNode(), i)
                && aff.isBackup(grid(2).localNode(), i)) {
                k2 = i;
                break;
            }
        }

        Integer key1 = k1, key2 = k2;

        assert key1 != null && key2 != null;

        // data initialization
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(key2));

        ImmutableMap<UUID, Set<UUID>> txNodes = ImmutableMap.of(
            grid(0).localNode().id(), Sets.newHashSet(grid(1).localNode().id(), grid(2).localNode().id()),
            grid(1).localNode().id(), Sets.newHashSet(grid(0).localNode().id(), grid(2).localNode().id())
        );

        // cache put
        checkScenario(ign, srvCnt, txNodes, () -> {
            cache.put(key1, 42);
            cache.put(key2, 42);
        });

        // fast update
        checkScenario(ign, srvCnt, txNodes, () -> {
            cache.query(new SqlFieldsQuery("merge into Integer(_key, _val) values(?, 42)").setArgs(key1));
            cache.query(new SqlFieldsQuery("merge into Integer(_key, _val) values(?, 42)").setArgs(key2));
        });

        // cursor update
        checkScenario(ign, srvCnt, txNodes, () -> {
            cache.query(new SqlFieldsQuery("update Integer set _val = _val + 1 where _key = ?").setArgs(key1));
            cache.query(new SqlFieldsQuery("update Integer set _val = _val + 1 where _key = ?").setArgs(key2));
        });

        // broadcast update
        checkScenario(ign, srvCnt, txNodes, () -> {
            cache.query(new SqlFieldsQuery("update Integer set _val = _val + 1").setArgs(key1));
        });

        // select for update does not start remote tx on backup
        ImmutableMap<UUID, Set<UUID>> sfuTxNodes = ImmutableMap.of(
            grid(0).localNode().id(), Collections.emptySet(),
            grid(1).localNode().id(), Collections.emptySet()
        );

        // cursor select for update
        checkScenario(ign, srvCnt, sfuTxNodes, () -> {
            cache.query(new SqlFieldsQuery("select _val from Integer where _key = ? for update").setArgs(key1)).getAll();
            cache.query(new SqlFieldsQuery("select _val from Integer where _key = ? for update").setArgs(key2)).getAll();
        });

        // broadcast select for update
        checkScenario(ign, srvCnt, sfuTxNodes, () -> {
            cache.query(new SqlFieldsQuery("select _val from Integer for update").setArgs(key1)).getAll();
        });
    }

    /** */
    private void checkScenario(IgniteEx ign, int srvCnt, ImmutableMap<UUID, Set<UUID>> txNodes, Runnable r)
        throws Exception {
        try (Transaction userTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            r.run();

            GridNearTxLocal nearTx = ((TransactionProxyImpl)userTx).tx();

            nearTx.prepareNearTxLocal().get();

            List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
                .mapToObj(i -> txsOnNode(grid(i), nearTx.nearXidVersion()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            assertFalse(txs.isEmpty());

            txs.forEach(tx -> assertEquals(txNodes, repack(tx.transactionNodes())));
        }
    }

    /** */
    private static CacheConfiguration<Object, Object> basicCcfg() {
        return new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class);
    }

    /** */
    private static Map<UUID, Set<UUID>> repack(Map<UUID, Collection<UUID>> orig) {
        ImmutableMap.Builder<UUID, Set<UUID>> builder = ImmutableMap.builder();

        orig.forEach((primary, backups) -> {
            builder.put(primary, new HashSet<>(backups));
        });

        return builder.build();
    }

    /** */
    private static List<IgniteInternalTx> txsOnNode(IgniteEx node, GridCacheVersion xidVer) {
        return node.context().cache().context().tm().activeTransactions().stream()
            .peek(tx -> assertEquals(xidVer, tx.nearXidVersion()))
            .collect(Collectors.toList());
    }
}
