package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.ThreadResolver;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;

public class MultiTxInOneThreadTest extends GridCommonAbstractTest {

    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.PARTITIONED).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setBackups(1).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        return cfg;
    }

    public void test() throws Exception {
        startGrids(4);

        client = true;

        IgniteEx client = startGrid(4);

        IgniteCache<Long, Long> cache = client.cache(DEFAULT_CACHE_NAME);

        cache.put(1L, 0L);
        cache.put(2L, 0L);
        cache.put(3L, 0L);
        cache.put(4L, 0L);

        long threadId1 = 1L;
        long threadId2 = 2L;

        long realThreadId = ThreadResolver.threadId();

        ThreadResolver.setThreadId(threadId1);

        Transaction tx1 = client.transactions().txStart();

        ThreadResolver.setThreadId(threadId2);

        Transaction tx2 = client.transactions().txStart();

        ThreadResolver.setThreadId(threadId1);

        Long val1 = cache.get(1L);
        Long val2 = cache.get(2L);

        ThreadResolver.setThreadId(threadId2);

        Long val3 = cache.get(3L);
        Long val4 = cache.get(4L);

        ThreadResolver.setThreadId(threadId1);

        cache.put(1L, val1 + 1);
        cache.put(2L, val2 + 1);

        ThreadResolver.setThreadId(threadId2);

        cache.put(3L, val3 + 1);
        cache.put(4L, val4 + 1);

        ThreadResolver.setThreadId(threadId1);

        tx1.commit();

        tx1.close();

        ThreadResolver.setThreadId(threadId2);

        tx2.commit();

        tx2.close();

        ThreadResolver.reset();

        Assert.assertEquals(1L, (long)cache.get(1L));
        Assert.assertEquals(1L, (long)cache.get(2L));
        Assert.assertEquals(1L, (long)cache.get(3L));
        Assert.assertEquals(1L, (long)cache.get(4L));

        Assert.assertEquals(realThreadId, ThreadResolver.threadId());

        stopAllGrids(false);
    }
}
