/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;

/**
 *
 */
public class GridCacheDistributedEvictionsSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private int gridCnt = 2;

    /** */
    private GridCacheMode mode;

    /** */
    private boolean nearEnabled;

    /** */
    private boolean evictSync;

    /** */
    private boolean evictNearSync;

    /** */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TransactionsConfiguration tCfg = new TransactionsConfiguration();

        tCfg.setDefaultTxConcurrency(PESSIMISTIC);
        tCfg.setDefaultTxIsolation(READ_COMMITTED);

        c.setTransactionsConfiguration(tCfg);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);

        // Set only DHT policy, leave default near policy.
        cc.setEvictionPolicy(new GridCacheFifoEvictionPolicy<>(10));
        cc.setEvictSynchronized(evictSync);
        cc.setEvictNearSynchronized(evictNearSync);
        cc.setEvictSynchronizedKeyBufferSize(1);

        cc.setAffinity(new GridCacheModuloAffinityFunction(gridCnt, 1));

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, idxGen.getAndIncrement()));

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Throwable If failed. */
    public void testNearSyncBackupUnsync() throws Throwable {
        gridCnt = 3;
        mode = PARTITIONED;
        evictNearSync = true;
        evictSync = false;
        nearEnabled = true;

        checkEvictions();
    }

    /** @throws Throwable If failed. */
    public void testNearSyncBackupSync() throws Throwable {
        gridCnt = 3;
        mode = PARTITIONED;
        evictNearSync = true;
        evictSync = true;
        nearEnabled = true;

        checkEvictions();
    }

    /** @throws Throwable If failed. */
    public void testNearUnsyncBackupSync() throws Throwable {
        gridCnt = 1;
        mode = PARTITIONED;
        evictNearSync = false;
        evictSync = true;
        nearEnabled = true;

        try {
            startGrid(0);

            assert false : "Grid was started with illegal configuration.";
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * http://atlassian.gridgain.com/jira/browse/GG-9002
     *
     * @throws Throwable If failed.
     */
    public void testLocalSync() throws Throwable {
        gridCnt = 1;
        mode = LOCAL;
        evictNearSync = true;
        evictSync = true;
        nearEnabled = true;

        Ignite g = startGrid(0);

        final GridCache<Integer, Integer> cache = g.cache(null);

        for (int i = 1; i < 20; i++) {
            cache.putx(i * gridCnt, i * gridCnt);

            info("Put to cache: " + i * gridCnt);
        }
    }

    /** @throws Throwable If failed. */
    private void checkEvictions() throws Throwable {
        try {
            startGrids(gridCnt);

            Ignite ignite = grid(0);

            final GridCache<Integer, Integer> cache = ignite.cache(null);

            // Put 1 entry to primary node.
            cache.putx(0, 0);

            Integer nearVal = this.<Integer, Integer>cache(2).get(0);

            assert nearVal == 0 : "Unexpected near value: " + nearVal;

            // Put several vals to primary node.
            for (int i = 1; i < 20; i++) {
                cache.putx(i * gridCnt, i * gridCnt);

                info("Put to cache: " + i * gridCnt);
            }

            for (int i = 0; i < 3; i++) {
                try {
                    assert cache(2).get(0) == null : "Entry has not been evicted from near node for key: " + 0;
                    assert cache(1).get(0) == null : "Entry has not been evicted from backup node for key: " + 0;
                    assert cache.get(0) == null : "Entry has not been evicted from primary node for key: " + 0;
                }
                catch (Throwable e) {
                    if (i == 2)
                        // No attempts left.
                        throw e;

                    U.warn(log, "Check failed (will retry in 2000 ms): " + e);

                    // Unwind evicts?
                    cache.get(0);

                    U.sleep(2000);
                }
            }

            for (int i = 0; i < 3; i++) {
                info("Primary key set: " + new TreeSet<>(this.<Integer, Integer>dht(0).keySet()));
                info("Primary near key set: " + new TreeSet<>(this.<Integer, Integer>near(0).keySet()));

                info("Backup key set: " + new TreeSet<>(this.<Integer, Integer>dht(1).keySet()));
                info("Backup near key set: " + new TreeSet<>(this.<Integer, Integer>near(1).keySet()));

                info("Near key set: " + new TreeSet<>(this.<Integer, Integer>dht(2).keySet()));
                info("Near node near key set: " + new TreeSet<>(this.<Integer, Integer>near(2).keySet()));

                try {
                    assert cache.size() == 10 : "Invalid cache size [size=" + cache.size() +
                        ", keys=" + new TreeSet<>(cache.keySet()) + ']';
                    assert cache.size() == 10 : "Invalid key size [size=" + cache.size() +
                        ", keys=" + new TreeSet<>(cache.keySet()) + ']';

                    assert cache(2).isEmpty();

                    break;
                }
                catch (Throwable e) {
                    if (i == 2)
                        // No attempts left.
                        throw e;

                    U.warn(log, "Check failed (will retry in 2000 ms): " + e);

                    // Unwind evicts?
                    cache.get(0);

                    U.sleep(2000);
                }
            }
        }
        catch (Throwable t) {
            error("Test failed.", t);

            throw t;
        }
        finally {
            stopAllGrids();
        }
    }
}
