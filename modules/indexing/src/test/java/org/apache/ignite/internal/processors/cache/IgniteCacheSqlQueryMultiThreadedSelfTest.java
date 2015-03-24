/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteCacheSqlQueryMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration<?,?> ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setNearConfiguration(null);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setIndexedTypes(
            Integer.class, Person.class
        );

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
        final IgniteCache<Integer, Person> cache = grid(0).cache(null);

        cache.clear();

        for (int i = 0; i < 2000; i++)
            cache.put(i, new Person(i));

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < 100; i++) {
                    QueryCursor<Cache.Entry<Integer, Person>> qry =
                        cache.query(new SqlQuery<Integer, Person>("Person", "age >= 0"));

                    int cnt = 0;

                    for (Cache.Entry<Integer, Person> e : qry)
                        cnt++;

                    assertEquals(2000, cnt);
                }

                return null;
            }
        }, 16, "test");
    }

    /**
     * Test put and parallel query.
     * @throws Exception If failed.
     */
    public void testQueryPut() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(0).cache(null);

        cache.clear();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Random rnd = new GridRandom();

                while (!stop.get()) {
                    List<List<?>> res = cache.query(
                        new SqlFieldsQuery("select avg(age) from Person where age > 0")).getAll();

                    assertEquals(1, res.size());

                    if (res.get(0).get(0) == null)
                        continue;

                    int avgAge = ((Number)res.get(0).get(0)).intValue();

                    if (rnd.nextInt(300) == 0)
                        X.println("__ " + avgAge);
                }

                return null;
            }
        }, 20);

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Random rnd = new GridRandom();
                Random age = new GridRandom();

                while (!stop.get())
                    cache.put(rnd.nextInt(2000), new Person(age.nextInt(3000) - 1000));

                return null;
            }
        }, 20);

        Thread.sleep(30 * 1000);

        stop.set(true);

        fut2.get(10 * 1000);
        fut1.get(10 * 1000);
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
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
