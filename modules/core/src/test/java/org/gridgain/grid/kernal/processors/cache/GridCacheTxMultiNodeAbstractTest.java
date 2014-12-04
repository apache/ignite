/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;


/**
 * Checks basic multi-node transactional operations.
 */
@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions", "PointlessArithmeticExpression"})
public abstract class GridCacheTxMultiNodeAbstractTest extends GridCommonAbstractTest {
    /** Debug flag. */
    private static final boolean DEBUG = false;

    /** */
    protected static final int GRID_CNT = 4;

    /** */
    protected static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int RETRIES = 300;

    /** Log frequency. */
    private static final int LOG_FREQ = RETRIES < 100 || DEBUG ? 1 : RETRIES / 5;

    /** Counter key. */
    private static final String CNTR_KEY = "CNTR_KEY";

    /** Removed counter key. */
    private static final String RMVD_CNTR_KEY = "RMVD_CNTR_KEY";

    /** */
    protected static final AtomicInteger cntr = new AtomicInteger();

    /** */
    protected static final AtomicInteger cntrRmvd = new AtomicInteger();

    /** Number of backups for partitioned tests. */
    protected int backups = 2;

     /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = 0;

        cntr.set(0);
    }

    /**
     *
     * @param ignite Grid
     * @param key Key.
     * @return Primary node id.
     */
    @SuppressWarnings("unchecked")
    private static UUID primaryId(Ignite ignite, Object key) {
        GridCacheAffinity aff = ignite.cache(null).cache().affinity();

        Collection<ClusterNode> affNodes = aff.mapPartitionToPrimaryAndBackups(aff.partition(key));

        ClusterNode first = F.first(affNodes);

        assert first != null;

        return first.id();
    }

    /**
     * @param nodeId Node ID.
     * @param key Key.
     * @return DHT entry.
     */
    @Nullable private static GridCacheEntryEx<Object, Integer> dhtEntry(UUID nodeId, Object key) {
        Ignite g = G.grid(nodeId);

        GridDhtCacheAdapter<Object, Integer> dht =
            ((GridKernal)g).<Object, Integer>internalCache().context().near().dht();

        return dht.peekEx(key);
    }

    /**
     * @param nodeId Node ID.
     * @param key Key.
     * @return Near entry.
     */
    @Nullable private static GridCacheEntryEx<Object, Integer> nearEntry(UUID nodeId, Object key) {
        Ignite g = G.grid(nodeId);

        GridNearCacheAdapter<Object, Integer> near = ((GridKernal)g).<Object, Integer>internalCache().context().near();

        return near.peekEx(key);
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param itemKey Item key.
     * @param retry Retry count.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private void onItemNear(boolean putCntr, Ignite ignite, String itemKey, int retry) throws GridException {
        GridCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID itemPrimaryId = primaryId(ignite, itemKey);
        UUID cntrPrimaryId = primaryId(ignite, CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                info("Before near get [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(CNTR_KEY);

            int newVal = cntr + 1;

            if (putCntr) {
                if (DEBUG)
                    info("Before near put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

                cache.putx(CNTR_KEY, newVal);
            }

            if (DEBUG)
                info("Before near put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr + ", new=" + newVal +
                    ", nearEntry=" + nearEntry(locId, itemKey) + ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            cache.putx(itemKey, newVal);

            if (DEBUG)
                info("After near put item [retry=" + retry + ", key=" + itemKey + ", old=" + cntr + ", new=" + newVal +
                    ", nearEntry=" + nearEntry(locId, itemKey) + ", dhtEntry" + dhtEntry(itemPrimaryId, itemKey) + ']');

            tx.commit();
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param itemKey Item key.
     * @param retry Retry count.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private void onItemPrimary(boolean putCntr, Ignite ignite, String itemKey, int retry) throws GridException {
        GridCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID itemPrimaryId = primaryId(ignite, itemKey);
        UUID cntrPrimaryId = primaryId(ignite, CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                info("Before item primary get [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(CNTR_KEY);

            int newVal = cntr + 1;

            if (putCntr) {
                if (DEBUG)
                    info("Before item primary put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

                cache.putx(CNTR_KEY, newVal);
            }

            if (DEBUG)
                info("Before item primary put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            cache.putx(itemKey, cntr);

            if (DEBUG)
                info("After item primary put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            tx.commit();
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param retry Retry count.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private void onRemoveItemQueried(boolean putCntr, Ignite ignite, int retry) throws GridException {
        GridCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID cntrPrimaryId = primaryId(ignite, RMVD_CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                ignite.log().info("Before item lock [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(RMVD_CNTR_KEY);

            assert cntr != null : "Received null counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']';

            int newVal = cntr - 1;

            if (putCntr) {
                if (DEBUG)
                    ignite.log().info("Before item put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

                cache.putx(RMVD_CNTR_KEY, newVal);
            }

            while (true) {
                GridCacheQuery<Map.Entry<String, Integer>> qry =
                    cache.queries().createSqlQuery(Integer.class, "_key != 'RMVD_CNTR_KEY' and _val >= 0");

                if (DEBUG)
                    ignite.log().info("Before executing query [retry=" + retry + ", locId=" + locId +
                        ", txId=" + tx.xid() + ']');

                Map.Entry<String, Integer> entry = qry.execute().next();

                if (entry == null) {
                    ignite.log().info("*** Queue is empty.");

                    return;
                }

                String itemKey = entry.getKey();

                UUID itemPrimaryId = primaryId(ignite, itemKey);

                // Lock the item key.
                if (cache.get(itemKey) != null) {
                    if (DEBUG)
                        ignite.log().info("Before item remove [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                            ", nearEntry=" + nearEntry(locId, itemKey) +
                            ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

                    assert cache.removex(itemKey) : "Failed to remove key [locId=" + locId +
                        ", primaryId=" + itemPrimaryId + ", key=" + itemKey + ']';

                    if (DEBUG)
                        info("After item remove item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                            ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                            ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

                    break;
                }
                else
                    cache.removex(itemKey);
            }

            tx.commit();
        }
        catch (Error e) {
            ignite.log().error("Error in test.", e);

            throw e;
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param retry Retry count.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private void onRemoveItemSimple(boolean putCntr, Ignite ignite, int retry) throws GridException {
        GridCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID cntrPrimaryId = primaryId(ignite, RMVD_CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                ignite.log().info("Before item lock [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(RMVD_CNTR_KEY);

            assert cntr != null : "Received null counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']';

            String itemKey = Integer.toString(cntrRmvd.getAndIncrement());

            Integer val = cache.get(itemKey);

            assert val != null : "Received null val [retry=" + retry + ", cacheSize=" + cache.size() + ']';

            UUID itemPrimaryId = primaryId(ignite, itemKey);

            int newVal = cntr - 1;

            if (putCntr) {
                if (DEBUG)
                    ignite.log().info("Before item put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

                cache.putx(RMVD_CNTR_KEY, newVal);
            }

            if (DEBUG)
                ignite.log().info("Before item remove item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            assertTrue(cache.removex(itemKey));

            if (DEBUG)
                info("After item put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            tx.commit();
        }
        catch (Error e) {
            ignite.log().error("Error in test.", e);

            throw e;
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @throws GridException If failed.
     */
    private void retries(Ignite ignite, boolean putCntr) throws GridException {
        UUID nodeId = ignite.cluster().localNode().id();

        for (int i = 0; i < RETRIES; i++) {
            int cnt = cntr.getAndIncrement();

            if (DEBUG)
                ignite.log().info("***");
            if (DEBUG || cnt % LOG_FREQ == 0)
                ignite.log().info("*** Iteration #" + i + " ***");
            if (DEBUG)
                ignite.log().info("***");

            String itemKey = nodeId + "-#" + i;

            if (nodeId.equals(primaryId(ignite, itemKey)))
                onItemPrimary(putCntr, ignite, itemKey, i);
            else
                onItemNear(putCntr, ignite, itemKey, i);
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @throws GridException If failed.
     */
    private void removeRetriesQueried(Ignite ignite, boolean putCntr) throws GridException {
        for (int i = 0; i < RETRIES; i++) {
            if (DEBUG)
                ignite.log().info("***");

            if (DEBUG || cntrRmvd.getAndIncrement() % LOG_FREQ == 0)
                ignite.log().info("*** Iteration #" + i + " ***");

            if (DEBUG)
                ignite.log().info("***");

            onRemoveItemQueried(putCntr, ignite, i);

            if (i % 50 == 0)
                ((GridKernal) ignite).internalCache().context().tm().printMemoryStats();
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @throws GridException If failed.
     */
    private void removeRetriesSimple(Ignite ignite, boolean putCntr) throws GridException {
        for (int i = 0; i < RETRIES; i++) {
            if (DEBUG)
                ignite.log().info("***");

            if (cntrRmvd.get() % LOG_FREQ == 0 || DEBUG)
                ignite.log().info("*** Iteration #" + i + " ***");

            if (DEBUG)
                ignite.log().info("***");

            onRemoveItemSimple(putCntr, ignite, i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutOneEntryInTx() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        try {
            grid(0).cache(null).put(CNTR_KEY, 0);

            grid(0).compute().call(new PutOneEntryInTxJob());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutTwoEntriesInTx() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        try {
            grid(0).cache(null).put(CNTR_KEY, 0);

            grid(0).compute().call(new PutTwoEntriesInTxJob());

            printCounter();

            assertEquals(GRID_CNT * RETRIES, grid(0).cache(null).get(CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutOneEntryInTxMultiThreaded() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        Collection<Thread> threads = new LinkedList<>();

        try {
            // Initialize.
            grid(0).cache(null).put(CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT; i++) {
                final int gridId = i;

                threads.add(new Thread("thread-#" + i) {
                    @Override public void run() {
                        try {
                            retries(grid(gridId), false);
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException(e);
                        }
                    }
                });
            }

            for (Thread th : threads)
                th.start();

            for (Thread th : threads)
                th.join();

            printCounter();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutTwoEntryInTxMultiThreaded() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        Collection<Thread> threads = new LinkedList<>();

        try {
            grid(0).cache(null).put(CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT; i++) {
                final int gridId = i;

                threads.add(new Thread() {
                    @Override public void run() {
                        try {
                            retries(grid(gridId), true);
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException(e);
                        }
                    }
                });
            }

            for (Thread th : threads)
                th.start();

            for (Thread th : threads)
                th.join();

            printCounter();

            assertEquals(GRID_CNT * RETRIES, grid(0).cache(null).get(CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveInTxQueried() throws Exception {
        //resetLog4j(Level.INFO, true, GridCacheTxManager.class.getPackage().getName());

        startGrids(GRID_CNT);

        try {
            GridCache<String, Integer> cache = grid(0).cache(null);

            cache.put(RMVD_CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                cache.put(String.valueOf(i), i);

            for (int i = 0; i < RETRIES; i++)
                for (int j = 0; j < GRID_CNT; j++)
                    assertEquals(i, grid(j).cache(null).get(String.valueOf(i)));

            GridCacheQuery<Map.Entry<String, Integer>> qry = cache.queries().createSqlQuery(Integer.class, " _val >= 0");

            Collection<Map.Entry<String, Integer>> entries = qry.execute().get();

            assertFalse(entries.isEmpty());

            cntrRmvd.set(0);

            grid(0).compute().call(new RemoveInTxJobQueried());

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                for (int ii = 0; ii < GRID_CNT; ii++)
                    assertEquals(null, grid(ii).cache(null).get(Integer.toString(i)));

            assertEquals(-GRID_CNT * RETRIES, grid(0).cache(null).peek(RMVD_CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveInTxSimple() throws Exception {
        startGrids(GRID_CNT);

        try {
            GridCache<String, Integer> cache = grid(0).cache(null);

            cache.put(RMVD_CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                cache.put(Integer.toString(i), i);

            for (int i = 0; i < RETRIES; i++)
                for (int j = 0; j < GRID_CNT; j++)
                    assertEquals(i, grid(j).cache(null).get(Integer.toString(i)));

            GridCacheQuery<Map.Entry<String, Integer>> qry = cache.queries().createSqlQuery(Integer.class, " _val >= 0");

            Collection<Map.Entry<String, Integer>> entries = qry.execute().get();

            assertFalse(entries.isEmpty());

            cntrRmvd.set(0);

            grid(0).compute().call(new RemoveInTxJobSimple());

            // Check using cache.
            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                for (int ii = 0; ii < GRID_CNT; ii++)
                    assertEquals(null, grid(ii).cache(null).get(Integer.toString(i)));

            // Check using query.
            entries = qry.execute().get();

            assertTrue(entries.isEmpty());

            assertEquals(-GRID_CNT * RETRIES, grid(0).cache(null).peek(RMVD_CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveInTxQueriedMultiThreaded() throws Exception {
        //resetLog4j(Level.INFO, true, GridCacheTxManager.class.getPackage().getName());

        backups = 1;

        try {
            startGrids(GRID_CNT);

            GridCache<String, Integer> cache = grid(0).cache(null);

            // Store counter.
            cache.put(RMVD_CNTR_KEY, 0);

            // Store values.
            for (int i = 1; i <= GRID_CNT * RETRIES; i++)
                cache.put(String.valueOf(i), i);

            for (int j = 0; j < GRID_CNT; j++)
                assertEquals(0, grid(j).cache(null).get(RMVD_CNTR_KEY));

            for (int i = 1; i <= RETRIES; i++)
                for (int j = 0; j < GRID_CNT; j++)
                    assertEquals(i, grid(j).cache(null).get(String.valueOf(i)));

            GridCacheQuery<Map.Entry<String, Integer>> qry = cache.queries().createSqlQuery(Integer.class, "_val >= 0");

            // Load all results.
            qry.keepAll(true);
            qry.includeBackups(false);

            // NOTE: for replicated cache includeBackups(false) is not enough since
            // all nodes are considered primary, so we have to deduplicate result set.
            if (cache.configuration().getCacheMode() == REPLICATED)
                qry.enableDedup(true);

            List<Map.Entry<String, Integer>> entries =
                new ArrayList<>(qry.execute().get());

            Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
                @Override public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            info("Queried entries: " + entries);

            int val = 0;

            for (Map.Entry<String, Integer> e : entries) {
                assertEquals(val, e.getValue().intValue());

                val++;
            }

            assertFalse(entries.isEmpty());

            cntrRmvd.set(0);

            Collection<Thread> threads = new LinkedList<>();

            for (int i = 0; i < GRID_CNT; i++) {
                final int gridId = i;

                threads.add(new Thread() {
                    @Override public void run() {
                        try {
                            removeRetriesQueried(grid(gridId), true);
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException(e);
                        }
                    }
                });
            }

            for (Thread th : threads)
                th.start();

            for (Thread th : threads)
                th.join();

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                for (int ii = 0; ii < GRID_CNT; ii++)
                    assertEquals("Got invalid value from cache [gridIdx=" + ii + ", key=" + i + ']',
                        null, grid(ii).cache(null).get(Integer.toString(i)));

            assertEquals(-GRID_CNT * RETRIES, grid(0).cache(null).peek(RMVD_CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws GridException If failed.
     */
    private void printCounter() throws GridException {
        info("***");
        info("*** Peeked counter: " + grid(0).cache(null).peek(CNTR_KEY));
        info("*** Got counter: " + grid(0).cache(null).get(CNTR_KEY));
        info("***");
    }

    /**
     * Test job putting data to queue.
     */
    protected class PutTwoEntriesInTxJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            retries(ignite, true);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutTwoEntriesInTxJob.class, this);
        }
    }

    /**
     * Test job putting data to cache.
     */
    protected class PutOneEntryInTxJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            retries(ignite, false);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutOneEntryInTxJob.class, this);
        }
    }

    /**
     * Test job removing data from cache using query.
     */
    protected class RemoveInTxJobQueried implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            removeRetriesQueried(ignite, true);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveInTxJobQueried.class, this);
        }
    }

    /**
     * Test job removing data from cache.
     */
    protected class RemoveInTxJobSimple implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            removeRetriesSimple(ignite, true);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveInTxJobSimple.class, this);
        }
    }
}
