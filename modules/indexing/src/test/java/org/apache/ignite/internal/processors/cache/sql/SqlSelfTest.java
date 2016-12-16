package org.apache.ignite.internal.processors.cache.sql;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collections;

/**
 * Created by yuryandreev on 28/10/2016.
 */
public abstract class SqlSelfTest extends GridCommonAbstractTest {

    protected IgniteCache<String, PersonTest> cache;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        cfg.setPeerClassLoadingEnabled(true);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     *
     */
    protected IgniteCache<String, PersonTest> createAndFillCache() {
        CacheConfiguration<String, PersonTest> cacheConf = new CacheConfiguration<>();
        cacheConf.setCacheMode(CacheMode.PARTITIONED);
        cacheConf.setBackups(0);

        cacheConf.setIndexedTypes(String.class, PersonTest.class);

        cacheConf.setName("PersonTest");

        cache = grid(0).createCache(cacheConf);
        initCacheData();

        return cache;
    }

    protected int getResultQuerySize(Query sqlQuery) {
        return cache.query(sqlQuery).getAll().size();
    }

    abstract void initCacheData();
}
