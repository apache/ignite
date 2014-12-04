/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.cache.query.GridCacheQueryType.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Continuous queries tests.
 */
public abstract class GridCacheContinuousQueryAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Latch timeout. */
    protected static final long LATCH_TIMEOUT = 5000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(peerClassLoadingEnabled());

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setDistributionMode(distributionMode());
        cacheCfg.setPreloadMode(ASYNC);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setStore(new TestStore());
        cacheCfg.setQueryIndexEnabled(false);

        cfg.setCacheConfiguration(cacheCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @return Peer class loading enabled flag.
     */
    protected boolean peerClassLoadingEnabled() {
        return true;
    }

    /**
     * @return Distribution.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    if (grid(i).nodes().size() != gridCount())
                        return false;
                }

                return true;
            }
        }, 3000);

        for (int i = 0; i < gridCount(); i++)
            assertEquals(gridCount(), grid(i).nodes().size());

        for (int i = 0; i < gridCount(); i++) {
            for (int j = 0; j < 5; j++) {
                try {
                    grid(i).cache(null).removeAll();

                    break;
                }
                catch (GridCachePartialUpdateException e) {
                    if (j == 4)
                        throw new Exception("Failed to clear cache for grid: " + i, e);

                    U.warn(log, "Failed to clear cache for grid (will retry in 500 ms) [gridIdx=" + i +
                        ", err=" + e.getMessage() + ']');

                    U.sleep(500);
                }
            }
        }

        for (int i = 0; i < gridCount(); i++)
            assertEquals("Cache is not empty: " + grid(i).cache(null).entrySet(), 0, grid(i).cache(null).size());

        for (int i = 0; i < gridCount(); i++) {
            GridContinuousProcessor proc = ((GridKernal)grid(i)).context().continuous();

            assertEquals(String.valueOf(i), 2, ((Map)U.field(proc, "locInfos")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "rmtInfos")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "startFuts")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "waitForStartAck")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "stopFuts")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "waitForStopAck")).size());
            assertEquals(String.valueOf(i), 0, ((Map)U.field(proc, "pending")).size());

            GridCacheContinuousQueryManager mgr =
                ((GridKernal)grid(i)).context().cache().internalCache().context().continuousQueries();

            assertEquals(0, ((Map)U.field(mgr, "lsnrs")).size());
        }
    }

    /**
     * @return Cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Grids count.
     */
    protected abstract int gridCount();

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testApi() throws Exception {
        final GridCacheContinuousQuery<Object, Object> q = grid(0).cache(null).queries().createContinuousQuery();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.bufferSize(-1);

                    return null;
                }
            },
            IllegalArgumentException.class,
            null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.bufferSize(0);

                    return null;
                }
            },
            IllegalArgumentException.class,
            null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.timeInterval(-1);

                    return null;
                }
            },
            IllegalArgumentException.class,
            null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.execute();

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );

        q.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Object, Object>>>() {
            @Override public boolean apply(UUID uuid, Collection<GridCacheContinuousQueryEntry<Object, Object>> entries) {
                return true;
            }
        });

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.execute(grid(0).forPredicate(F.<ClusterNode>alwaysFalse()));

                    return null;
                }
            },
            GridTopologyException.class,
            null
        );

        q.execute();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Object, Object>>>() {
                        @Override public boolean apply(UUID uuid, Collection<GridCacheContinuousQueryEntry<Object, Object>> entries) {
                            return false;
                        }
                    });

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.remoteFilter(null);

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.bufferSize(10);

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.timeInterval(10);

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );

        q.close();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    q.execute();

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllEntries() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId,
                Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            qry.execute();

            cache.putx(1, 1);
            cache.putx(2, 2);
            cache.putx(3, 3);

            cache.removex(2);

            cache.putx(1, 10);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(3, map.size());

            List<Integer> vals = map.get(1);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(1, (int)vals.get(0));
            assertEquals(10, (int)vals.get(1));

            vals = map.get(2);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(2, (int)vals.get(0));
            assertNull(vals.get(1));

            vals = map.get(3);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(3, (int)vals.get(0));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesByFilter() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(4);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return true;
            }
        });

        qry.remoteFilter(new P1<GridCacheContinuousQueryEntry<Integer, Integer>>() {
            @Override public boolean apply(GridCacheContinuousQueryEntry<Integer, Integer> e) {
                return e.getKey() > 2;
            }
        });

        // Second query to wait for notifications about all updates.
        GridCacheContinuousQuery<Integer, Integer> qry0 = cache.queries().createContinuousQuery();

        final CountDownLatch latch0 = new CountDownLatch(8);

        qry0.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID uuid,
                Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> ignored : entries)
                    latch0.countDown();

                return true;
            }
        });

        try {
            qry.execute();
            qry0.execute();

            cache.putx(1, 1);
            cache.putx(2, 2);
            cache.putx(3, 3);
            cache.putx(4, 4);

            cache.removex(2);
            cache.removex(3);

            cache.putx(1, 10);
            cache.putx(4, 40);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            List<Integer> vals = map.get(3);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(3, (int)vals.get(0));
            assertNull(vals.get(1));

            vals = map.get(4);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(4, (int)vals.get(0));
            assertEquals(40, (int)vals.get(1));

            assert latch0.await(2, SECONDS);
        }
        finally {
            qry.close();
            qry0.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjection() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // Queries for non-partitioned caches always run locally.
        if (cache.configuration().getCacheMode() != PARTITIONED)
            return;

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            qry.execute(grid(0).forRemotes());

            int locKey = -1;
            int rmtKey = -1;

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(grid(0).localNode()))
                    locKey = key;
                else
                    rmtKey = key;

                key++;

                if (locKey >= 0 && rmtKey >= 0)
                    break;
            }

            cache.putx(locKey, 1);
            cache.putx(rmtKey, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(1, map.size());

            List<Integer> vals = map.get(rmtKey);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(2, (int)vals.get(0));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalNodeOnly() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // Queries for non-partitioned caches always run locally.
        if (cache.configuration().getCacheMode() != PARTITIONED)
            return;

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            qry.execute(grid(0).forLocal());

            int locKey = -1;
            int rmtKey = -1;

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(grid(0).localNode()))
                    locKey = key;
                else
                    rmtKey = key;

                key++;

                if (locKey >= 0 && rmtKey >= 0)
                    break;
            }

            cache.putx(locKey, 1);
            cache.putx(rmtKey, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(1, map.size());

            List<Integer> vals = map.get(locKey);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(1, (int)vals.get(0));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopByCallback() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return false;
            }
        });

        // Second query to wait for notifications about all updates.
        GridCacheContinuousQuery<Integer, Integer> qry0 = cache.queries().createContinuousQuery();

        final CountDownLatch latch0 = new CountDownLatch(3);

        qry0.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId,
                Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> ignored : entries)
                    latch0.countDown();

                return true;
            }
        });

        try {
            qry.execute();
            qry0.execute();

            cache.putx(1, 1);
            cache.putx(2, 2);
            cache.putx(3, 3);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(1, map.size());

            List<Integer> list = F.first(map.values());

            assert list != null;

            assertEquals(1, list.size());

            assert latch0.await(2, SECONDS);
        }
        finally {
            qry.close();
            qry0.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBuffering() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // Buffering make sense only for remote nodes, so test only for partitioned cache.
        if (cache.configuration().getCacheMode() != PARTITIONED)
            return;

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return true;
            }
        });

        qry.bufferSize(5);

        try {
            ClusterNode node = F.first(grid(0).forRemotes().nodes());

            qry.execute(grid(0).forNode(node));

            Collection<Integer> keys = new HashSet<>();

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(node))
                    keys.add(key);

                key++;

                if (keys.size() == 6)
                    break;
            }

            Iterator<Integer> it = keys.iterator();

            for (int i = 0; i < 4; i++)
                cache.putx(it.next(), 0);

            assert !latch.await(2, SECONDS);

            for (int i = 0; i < 2; i++)
                cache.putx(it.next(), 0);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(5, map.size());

            it = keys.iterator();

            for (int i = 0; i < 5; i++) {
                Integer k = it.next();

                List<Integer> vals = map.get(k);

                assertNotNull(vals);
                assertEquals(1, vals.size());
                assertEquals(0, (int)vals.get(0));
            }
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimeInterval() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // Buffering make sense only for remote nodes, so test only for partitioned cache.
        if (cache.configuration().getCacheMode() != PARTITIONED)
            return;

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }

                return true;
            }
        });

        qry.bufferSize(10);
        qry.timeInterval(3000);

        try {
            ClusterNode node = F.first(grid(0).forRemotes().nodes());

            Collection<Integer> keys = new HashSet<>();

            int key = 0;

            while (true) {
                ClusterNode n = grid(0).mapKeyToNode(null, key);

                assert n != null;

                if (n.equals(node))
                    keys.add(key);

                key++;

                if (keys.size() == 5)
                    break;
            }

            for (Integer k : keys)
                cache.putx(k, 0);

            qry.execute(grid(0).forNode(node));

            assert !latch.await(2, SECONDS);
            assert latch.await(1000 + LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(5, map.size());

            Iterator<Integer> it = keys.iterator();

            for (int i = 0; i < 5; i++) {
                Integer k = it.next();

                List<Integer> vals = map.get(k);

                assertNotNull(vals);
                assertEquals(1, vals.size());
                assertEquals(0, (int)vals.get(0));
            }
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteration() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId,
                Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            for (int i = 0; i < 10; i++)
                cache.putx(i, i);

            qry.execute();

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(10, map.size());

            for (int i = 0; i < 10; i++)
                assertEquals(i, (int)map.get(i));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterationAndUpdates() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(12);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            for (int i = 0; i < 10; i++)
                cache.putx(i, i);

            qry.execute();

            cache.putx(10, 10);
            cache.putx(11, 11);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : latch.getCount();

            assertEquals(12, map.size());

            for (int i = 0; i < 12; i++)
                assertEquals(i, (int)map.get(i));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            qry.execute();

            for (int i = 0; i < gridCount(); i++)
                grid(i).cache(null).loadCache(null, 0);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : "Count: " + latch.getCount();

            assertEquals(10, map.size());

            for (int i = 0; i < 10; i++)
                assertEquals(i, (int)map.get(i));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypedProjection() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry =
            cache.projection(Integer.class, Integer.class).queries().createContinuousQuery();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        qry.remoteFilter(new P1<GridCacheContinuousQueryEntry<Integer, Integer>>() {
            @Override public boolean apply(GridCacheContinuousQueryEntry<Integer, Integer> e) {
                return true;
            }
        });

        try {
            qry.execute();

            cache.putx(1, 1);
            cache.putx("a", "a");
            cache.putx(2, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            assertEquals(1, (int)map.get(1));
            assertEquals(2, (int)map.get(2));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntryFilterProjection() throws Exception {
        GridCacheProjection<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.projection(
            new P1<GridCacheEntry<Integer, Integer>>() {
                @Override public boolean apply(GridCacheEntry<Integer, Integer> e) {
                    Integer i = e.peek();

                    return i != null && i > 10;
                }
            }).queries().createContinuousQuery();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (GridCacheContinuousQueryEntry<Integer, Integer> e : entries) {
                    info("Query entry: " + e);

                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        qry.remoteFilter(new P1<GridCacheContinuousQueryEntry<Integer, Integer>>() {
            @Override public boolean apply(GridCacheContinuousQueryEntry<Integer, Integer> e) {
                return true;
            }
        });

        try {
            qry.execute();

            cache.putx(1, 1);
            cache.putx(11, 11);
            cache.putx(2, 2);
            cache.putx(22, 22);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals("Invalid number of entries notified: " + map, 2, map.size());

            assertEquals(11, (int)map.get(11));
            assertEquals(22, (int)map.get(22));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeyValueFilterProjection() throws Exception {
        GridCacheProjection<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.projection(
            new P2<Integer, Integer>() {
                @Override public boolean apply(Integer key, Integer val) {
                    return val > 10;
                }
            }).queries().createContinuousQuery();

        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                for (Map.Entry<Integer, Integer> e : entries) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        qry.remoteFilter(new P1<GridCacheContinuousQueryEntry<Integer, Integer>>() {
            @Override public boolean apply(GridCacheContinuousQueryEntry<Integer, Integer> e) {
                return true;
            }
        });

        try {
            qry.execute();

            cache.putx(1, 1);
            cache.putx(11, 11);
            cache.putx(2, 2);
            cache.putx(22, 22);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            assertEquals(11, (int)map.get(11));
            assertEquals(22, (int)map.get(22));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInternalKey() throws Exception {
        if (atomicityMode() == ATOMIC)
            return;

        GridCache<Object, Object> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Object, Object> qry = cache.queries().createContinuousQuery();

        final Map<Object, Object> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Object, Object>>>() {
            @Override public boolean apply(UUID nodeId,
                Collection<GridCacheContinuousQueryEntry<Object, Object>> entries) {
                for (Map.Entry<Object, Object> e : entries) {
                    map.put(e.getKey(), e.getValue());

                    latch.countDown();
                }

                return true;
            }
        });

        try {
            qry.execute();

            cache.dataStructures().atomicLong("long", 0, true);

            cache.putx(1, 1);
            cache.putx(2, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, map.size());

            assertEquals(1, (int)map.get(1));
            assertEquals(2, (int)map.get(2));
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If filter.
     */
    public void testUpdateInFilter() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.putx(1, 1);

        GridCacheProjection<Integer, Integer> prj = cache.projection(new P1<GridCacheEntry<Integer, Integer>>() {
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override public boolean apply(final GridCacheEntry<Integer, Integer> e) {
                GridTestUtils.assertThrows(
                    log,
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            e.set(1000);

                            return null;
                        }
                    },
                    GridCacheFlagException.class,
                    null
                );

                return true;
            }
        });

        GridCacheContinuousQuery<Integer, Integer> qry = prj.queries().createContinuousQuery();

        final CountDownLatch latch = new CountDownLatch(1);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                latch.countDown();

                return true;
            }
        });

        try {
            qry.execute();

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);
        }
        finally {
            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoin() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

        final Collection<Map.Entry<Integer, Integer>> all = new ConcurrentLinkedDeque8<>();
        final CountDownLatch latch = new CountDownLatch(2);

        qry.localCallback(new P2<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
            @Override public boolean apply(UUID nodeId, Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                assertEquals(1, entries.size());

                all.addAll(entries);

                latch.countDown();

                return true;
            }
        });

        qry.execute();

        cache.putx(1, 1);

        try {
            startGrid("anotherGrid");

            cache.putx(2, 2);

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS) : all;

            assertEquals(2, all.size());
        }
        finally {
            stopGrid("anotherGrid");

            qry.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallbackForPreload() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        if (cache.configuration().getCacheMode() == LOCAL)
            return;

        Map<Integer, Integer> map = new HashMap<>();

        final int keysCnt = 1000;

        for (int i = 0; i < keysCnt; i++)
            map.put(i, i);

        cache.putAll(map);

        Ignite ignite = startGrid("anotherGrid");

        try {
            cache = ignite.cache(null);

            GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery();

            final CountDownLatch latch = new CountDownLatch(1);
            final Collection<Integer> keys = new GridConcurrentHashSet<>();

            qry.localCallback(new IgniteBiPredicate<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
                @Override public boolean apply(UUID nodeId,
                    Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                    for (Map.Entry<Integer, Integer> e : entries) {
                        keys.add(e.getKey());

                        if (keys.size() >= keysCnt)
                            latch.countDown();
                    }

                    return true;
                }
            });

            qry.execute();

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(keysCnt, keys.size());

            qry.close();
        }
        finally {
            stopGrid("anotherGrid");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvents() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(50);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        IgnitePredicate<IgniteEvent> lsnr = new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                assert evt instanceof IgniteCacheQueryReadEvent;

                IgniteCacheQueryReadEvent qe = (IgniteCacheQueryReadEvent)evt;

                assertEquals(CONTINUOUS, qe.queryType());
                assertNull(qe.cacheName());

                assertEquals(grid(0).localNode().id(), qe.subjectId());

                assertNull(qe.className());
                assertNull(qe.clause());
                assertNull(qe.scanQueryFilter());
                assertNotNull(qe.continuousQueryFilter());
                assertNull(qe.arguments());

                cnt.incrementAndGet();
                latch.countDown();

                return true;
            }
        };

        IgnitePredicate<IgniteEvent> execLsnr = new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                assert evt instanceof IgniteCacheQueryExecutedEvent;

                IgniteCacheQueryExecutedEvent qe = (IgniteCacheQueryExecutedEvent)evt;

                assertEquals(CONTINUOUS, qe.queryType());
                assertNull(qe.cacheName());

                assertEquals(grid(0).localNode().id(), qe.subjectId());

                assertNull(qe.className());
                assertNull(qe.clause());
                assertNull(qe.scanQueryFilter());
                assertNotNull(qe.continuousQueryFilter());
                assertNull(qe.arguments());

                execLatch.countDown();

                return true;
            }
        };

        try {
            for (int i = 0; i < gridCount(); i++) {
                grid(i).events().localListen(lsnr, EVT_CACHE_QUERY_OBJECT_READ);
                grid(i).events().localListen(execLsnr, EVT_CACHE_QUERY_EXECUTED);
            }

            GridCache<Integer, Integer> cache = grid(0).cache(null);

            try (GridCacheContinuousQuery<Integer, Integer> qry = cache.queries().createContinuousQuery()) {
                qry.localCallback(new IgniteBiPredicate<UUID, Collection<GridCacheContinuousQueryEntry<Integer, Integer>>>() {
                    @Override public boolean apply(UUID uuid,
                        Collection<GridCacheContinuousQueryEntry<Integer, Integer>> entries) {
                        return true;
                    }
                });

                qry.remoteFilter(new IgnitePredicate<GridCacheContinuousQueryEntry<Integer, Integer>>() {
                    @Override public boolean apply(GridCacheContinuousQueryEntry<Integer, Integer> e) {
                        return e.getValue() >= 50;
                    }
                });

                qry.execute();

                for (int i = 0; i < 100; i++)
                    cache.putx(i, i);

                assert latch.await(LATCH_TIMEOUT, MILLISECONDS);
                assert execLatch.await(LATCH_TIMEOUT, MILLISECONDS);

                assertEquals(50, cnt.get());
            }
        }
        finally {
            for (int i = 0; i < gridCount(); i++) {
                grid(i).events().stopLocalListen(lsnr, EVT_CACHE_QUERY_OBJECT_READ);
                grid(i).events().stopLocalListen(execLsnr, EVT_CACHE_QUERY_EXECUTED);
            }
        }
    }

    /**
     * Store.
     */
    private static class TestStore extends GridCacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo,
            Object... args) throws GridException {
            for (int i = 0; i < 10; i++)
                clo.apply(i, i);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(@Nullable GridCacheTx tx, Object key)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable GridCacheTx tx, Object key,
            @Nullable Object val) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable GridCacheTx tx, Object key)
            throws GridException {
            // No-op.
        }
    }
}
