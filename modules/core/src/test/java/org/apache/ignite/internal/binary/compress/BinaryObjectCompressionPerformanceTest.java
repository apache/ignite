package org.apache.ignite.internal.binary.compress;

import java.util.Date;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CompressionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class BinaryObjectCompressionPerformanceTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final long LARGE_PRIME = 4294967291L;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    public void test() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache c = ignite.createCache(new CacheConfiguration<>("foo")
            .setCompressionConfiguration(new CompressionConfiguration()));
        long time = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            long seed = (i * i) % LARGE_PRIME;
            c.put(new TestKey(seed), new TestObject(seed));
            if (i > 100000) {
                int idToRemove = i - 100000;
                c.remove(new TestKey((idToRemove * idToRemove) % LARGE_PRIME));
            }
        }
        System.err.println("Time took: " + ((System.currentTimeMillis() - time) / 1000));

        ignite.close();
    }

    /**
     *
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject {
        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int iVal;

        /** */
        private long lVal;

        /** */
        private float fVal;

        /** */
        private double dVal;

        /** */
        private String strVal;

        /** */
        private String dateVal;


        /**
         * @param seed Seed.
         */
        private TestObject(long seed) {
            bVal = (byte)seed;
            cVal = (char)seed;
            sVal = (short)seed;
            strVal = Long.toString(seed);
            iVal = (int)seed;
            lVal = seed;
            fVal = seed;
            dVal = seed;
            dateVal = new Date(seed).toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestKey {
        /** */
        private String key;

        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param seed Seed.
         */
        private TestKey(long seed) {
            key = Long.toHexString(seed);
            affKey = 0xfff & (int)seed;
        }
    }
}
