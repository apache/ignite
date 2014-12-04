/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public class GridCacheSqlQueryMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridH2IndexingSpi indexing = new GridH2IndexingSpi();

        c.setIndexingSpi(indexing);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setDistributionMode(PARTITIONED_ONLY);
        ccfg.setQueryIndexEnabled(true);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        c.setCacheConfiguration(ccfg);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuery() throws Exception {
        final GridCache<Integer, Person> cache = grid(0).cache(null);

        for (int i = 0; i < 2000; i++)
            cache.put(i, new Person(i));

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < 100; i++) {
                    GridCacheQuery<Map.Entry<Integer, Person>> qry =
                        cache.queries().createSqlQuery("Person", "age >= 0");

                    qry.includeBackups(false);
                    qry.enableDedup(true);
                    qry.keepAll(true);
                    qry.pageSize(50);

                    GridCacheQueryFuture<Map.Entry<Integer, Person>> fut = qry.execute();

                    int cnt = 0;

                    while (fut.next() != null)
                        cnt++;

                    assertEquals(2000, cnt);
                }

                return null;
            }
        }, 16, "test");
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @GridCacheQuerySqlField
        private int age;

        /**
         * @param age Age.
         */
        Person(int age) {
            this.age = age;
        }

        /**
         * @return Age/
         */
        public int age() {
            return age;
        }
    }
}
