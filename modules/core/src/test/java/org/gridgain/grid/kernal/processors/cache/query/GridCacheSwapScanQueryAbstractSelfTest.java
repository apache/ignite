/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public abstract class GridCacheSwapScanQueryAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new GridFileSwapSpaceSpi());

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setSwapEnabled(true);

        ccfg.setMemoryMode(OFFHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(10);

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setAtomicityMode(ATOMIC);

        ccfg.setPortableEnabled(portableEnabled());

        if (portableEnabled()) {
            GridPortableConfiguration pCfg = new GridPortableConfiguration();

            pCfg.setClassNames(Arrays.asList(Key.class.getName(), Person.class.getName()));

            cfg.setPortableConfiguration(pCfg);
        }

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Portable enabled flag.
     */
    protected abstract boolean portableEnabled();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testQuery() throws Exception {
        GridCache<Key, Person> cache = grid(0).cache(null);

        for (int i = 0; i < 100; i++)
            cache.putx(new Key(i), new Person("p-" + i, i));

        try {
            GridCacheQuery<Map.Entry<Key, Person>> qry = cache.queries().createScanQuery(new GridBiPredicate<Key, Person>() {
                @Override public boolean apply(Key key, Person p) {
                    assertEquals(key.id, (Integer)p.salary);

                    return key.id % 2 == 0;
                }
            });

            Collection<Map.Entry<Key, Person>> res = qry.execute().get();

            assertEquals(50, res.size());

            for (Map.Entry<Key, Person> e : res) {
                Key k = e.getKey();
                Person p = e.getValue();

                assertEquals(k.id, (Integer)p.salary);
                assertEquals(0, k.id % 2);
            }
        }
        finally {
            for (int i = 0; i < 10; i++)
                cache.removex(new Key(i));
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Key {
        /** */
        @SuppressWarnings("PublicField")
        public Integer id;

        /**
         * @param id ID.
         */
        public Key(Integer id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return id.equals(key.id);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Person {
        /** */
        @SuppressWarnings("PublicField")
        public String name;

        /** */
        @SuppressWarnings("PublicField")
        public int salary;

        /**
         * @param name Name.
         * @param salary Salary.
         */
        public Person(String name, int salary) {
            this.name = name;
            this.salary = salary;
        }
    }
}
