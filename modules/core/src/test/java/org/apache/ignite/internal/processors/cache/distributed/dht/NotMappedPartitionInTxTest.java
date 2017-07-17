package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
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
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 */
@SuppressWarnings("unchecked")
public class NotMappedPartitionInTxTest extends GridCommonAbstractTest {
    /** Cache. */
    private static final String CACHE = "testCache";
    /** Cache 2. */
    public static final String CACHE2 = CACHE + 1;
    /** Test key. */
    private static final String TEST_KEY = "key";
    /** Is client. */
    private boolean isClient = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setClientMode(isClient)
            .setCacheConfiguration(
                new CacheConfiguration(CACHE)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setCacheMode(CacheMode.REPLICATED)
                    .setAffinity(new TestAffinity()),
                new CacheConfiguration(CACHE2)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /**
     *
     */
    public void testOneServerOptimistic() throws Exception {
        try {
            isClient = false;
            startGrid(0);

            isClient = true;
            final IgniteEx client = startGrid(1);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    testNotMapped(client, OPTIMISTIC, REPEATABLE_READ);

                    return null;
                }
            }, ClusterTopologyServerNotFoundException.class, "Failed to map keys to nodes (partition is not mapped to any node)");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testOneServerOptimisticSerializable() throws Exception {
        try {
            isClient = false;
            startGrid(0);

            isClient = true;
            final IgniteEx client = startGrid(1);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    testNotMapped(client, OPTIMISTIC, SERIALIZABLE);

                    return null;
                }
            }, ClusterTopologyServerNotFoundException.class, "Failed to map keys to nodes (partition is not mapped to any node)");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testOneServerPessimistic() throws Exception {
        try {
            isClient = false;
            startGrid(0);

            isClient = true;
            final IgniteEx client = startGrid(1);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    testNotMapped(client, PESSIMISTIC, READ_COMMITTED);

                    return null;
                }
            }, ClusterTopologyServerNotFoundException.class, "Failed to lock keys (all partition nodes left the grid)");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testFourServersOptimistic() throws Exception {
        try {
            isClient = false;
            startGrids(4);

            isClient = true;
            final IgniteEx client = startGrid(4);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    testNotMapped(client, OPTIMISTIC, REPEATABLE_READ);

                    return null;
                }
            }, ClusterTopologyServerNotFoundException.class, "Failed to map keys to nodes (partition is not mapped to any node)");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testFourServersOptimisticSerializable() throws Exception {
        try {
            isClient = false;
            startGrids(4);

            isClient = true;
            final IgniteEx client = startGrid(4);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    testNotMapped(client, OPTIMISTIC, SERIALIZABLE);

                    return null;
                }
            }, ClusterTopologyServerNotFoundException.class, "Failed to map keys to nodes (partition is not mapped to any node)");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testFourServersPessimistic() throws Exception {
        try {
            isClient = false;
            startGrids(4);

            isClient = true;
            final IgniteEx client = startGrid(4);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    testNotMapped(client, PESSIMISTIC, READ_COMMITTED);

                    return null;
                }
            }, ClusterTopologyServerNotFoundException.class, "Failed to lock keys (all partition nodes left the grid)");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param client Ignite client.
     */
    private void testNotMapped(IgniteEx client, TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache2 = client.cache(CACHE2);
        IgniteCache cache1 = client.cache(CACHE).withKeepBinary();

        try(Transaction tx = client.transactions().txStart(concurrency, isolation)) {

            Map<String, Integer> param = new TreeMap<>();
            param.put(TEST_KEY + 1, 1);
            param.put(TEST_KEY + 1, 3);
            param.put(TEST_KEY, 3);

            cache1.put(TEST_KEY, 3);

            cache1.putAll(param);
            cache2.putAll(param);

            tx.commit();
        }
    }

    /** */
    private static class TestAffinity extends RendezvousAffinityFunction implements Serializable{
        /** */
        TestAffinity() {
        }

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
