package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

public class IgniteReentrantLockPartitionedTest extends IgniteReentrantLockTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        assertNotNull(atomicCfg);

        atomicCfg.setCacheMode(CacheMode.PARTITIONED);
        atomicCfg.setBackups(NODES_CNT-1);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }
}
