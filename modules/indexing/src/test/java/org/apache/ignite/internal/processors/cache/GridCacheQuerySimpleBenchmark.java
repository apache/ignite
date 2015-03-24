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
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jsr166.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridCacheQuerySimpleBenchmark extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration<?,?> ccfg = new CacheConfiguration<>();

        ccfg.setName("offheap-cache");
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setSwapEnabled(false);
        ccfg.setIndexedTypes(
            Long.class, Person.class
        );

        ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);

        c.setCacheConfiguration(ccfg);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignite = null;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPerformance() throws Exception {
        Random rnd = new GridRandom();

        final IgniteCache<Long,Person> c = ignite.cache("offheap-cache");

        X.println("___ PUT start");

        final int cnt = 100_000;
        final int maxSalary = cnt / 10;

        for (long i = 0; i < cnt; i++)
            c.put(i, new Person(rnd.nextInt(maxSalary), "Vasya " + i));

        X.println("___ PUT end");

        final AtomicBoolean end = new AtomicBoolean();

        final LongAdder8 puts = new LongAdder8();

        IgniteInternalFuture<?> fut0 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Random rnd = new GridRandom();

                while (!end.get()) {
                    long i = rnd.nextInt(cnt);

                    c.put(i, new Person(rnd.nextInt(maxSalary), "Vasya " + i));

                    puts.increment();
                }

                return null;
            }
        }, 10);

        final LongAdder8 qrys = new LongAdder8();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Random rnd = new GridRandom();

                while (!end.get()) {
                    int salary = rnd.nextInt(maxSalary);

                    c.query(new SqlFieldsQuery("select name from Person where salary = ?").setArgs(salary))
                        .getAll();

                    qrys.increment();
                }

                return null;
            }
        }, 10);

        int runTimeSec = 600;

        for (int s = 0; s < runTimeSec; s++) {
            Thread.sleep(1000);

            long puts0 = puts.sum();
            long qrys0 = qrys.sum();

            puts.add(-puts0);
            qrys.add(-qrys0);

            X.println("___ puts: " + puts0 + " qrys: " + qrys0);
        }

        end.set(true);

        fut0.get();
        fut1.get();

        X.println("___ STOP");
    }

    /**
     *
     */
    private static class Person implements Externalizable {
        /** */
        @QuerySqlField(index = true)
        int salary;

        /** */
        @QuerySqlField
        String name;

        /**
         *
         */
        public Person() {
            // No-op.
        }

        /**
         * @param salary Salary.
         * @param name Name.
         */
        Person(int salary, String name) {
            this.salary = salary;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(salary);
            U.writeString(out, name);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            salary = in.readInt();
            name = U.readString(in);
        }
    }
}
