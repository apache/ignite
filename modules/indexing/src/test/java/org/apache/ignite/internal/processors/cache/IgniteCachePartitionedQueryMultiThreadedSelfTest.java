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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for partitioned cache queries.
 */
public class IgniteCachePartitionedQueryMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** Number of test grids (nodes). Should not be less than 2. */
    private static final int GRID_CNT = 3;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Don't start grid by default. */
    public IgniteCachePartitionedQueryMultiThreadedSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);

        // Query should be executed without ongoing transactions.
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setBackups(0);
        cc.setRebalanceMode(CacheRebalanceMode.SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setIndexedTypes(
            UUID.class, PersonObj.class
        );

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assert GRID_CNT >= 2 : "Constant GRID_CNT must be greater than or equal to 2.";

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Clean up all caches.
        for (int i = 0; i < GRID_CNT; i++)
            grid(i).cache(null).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected void info(String msg) {
        if (TEST_INFO)
            super.info(msg);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLuceneAndSqlMultithreaded() throws Exception {
        // ---------- Test parameters ---------- //
        int luceneThreads = 10;
        int sqlThreads = 10;
        long duration = 10 * 1000;
        final int logMod = 100;

        final PersonObj p1 = new PersonObj("Jon", 1500, "Master");
        final PersonObj p2 = new PersonObj("Jane", 2000, "Master");
        final PersonObj p3 = new PersonObj("Mike", 1800, "Bachelor");
        final PersonObj p4 = new PersonObj("Bob", 1900, "Bachelor");

        final IgniteCache<UUID, PersonObj> cache0 = grid(0).cache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.localSize(CachePeekMode.ALL));

        assert grid(0).cluster().nodes().size() == GRID_CNT;

        final AtomicBoolean done = new AtomicBoolean();

        final AtomicLong luceneCnt = new AtomicLong();

        // Start lucene query threads.
        IgniteInternalFuture<?> futLucene = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (!done.get()) {
                    QueryCursor<Cache.Entry<UUID, PersonObj>> master =
                        cache0.query(new TextQuery(PersonObj.class, "Master"));

                    Collection<Cache.Entry<UUID, PersonObj>> entries = master.getAll();

                    checkResult(entries, p1, p2);

                    long cnt = luceneCnt.incrementAndGet();

                    if (cnt % logMod == 0)
                        info("Executed LUCENE queries: " + cnt);
                }
            }
        }, luceneThreads, "LUCENE-THREAD");

        final AtomicLong sqlCnt = new AtomicLong();

        // Start sql query threads.
        IgniteInternalFuture<?> futSql = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (!done.get()) {
                    QueryCursor<Cache.Entry<UUID, PersonObj>> bachelors =
                            cache0.query(new SqlQuery(PersonObj.class, "degree = 'Bachelor'"));

                    Collection<Cache.Entry<UUID, PersonObj>> entries = bachelors.getAll();

                    checkResult(entries, p3, p4);

                    long cnt = sqlCnt.incrementAndGet();

                    if (cnt % logMod == 0)
                        info("Executed SQL queries: " + cnt);
                }
            }
        }, sqlThreads, "SQL-THREAD");

        Thread.sleep(duration);

        done.set(true);

        futLucene.get();
        futSql.get();
    }

    /**
     * @param entries Queried result.
     * @param persons Persons that should be in the result.
     */
    private void checkResult(Iterable<Cache.Entry<UUID, PersonObj>> entries, PersonObj... persons) {
        for (Cache.Entry<UUID, PersonObj> entry : entries) {
            assertEquals(entry.getKey(), entry.getValue().id());

            assert F.asList(persons).contains(entry.getValue());
        }
    }

    /** Test class. */
    private static class PersonObj implements Externalizable {
        /** */
        @GridToStringExclude
        private UUID id = UUID.randomUUID();

        /** */
        @QuerySqlField
        private String name;

        /** */
        @QuerySqlField
        private int salary;

        /** */
        @QuerySqlField
        @QueryTextField
        private String degree;

        /** Required by {@link Externalizable}. */
        public PersonObj() {
            // No-op.
        }

        /**
         * @param name Name.
         * @param salary Salary.
         * @param degree Degree.
         */
        PersonObj(String name, int salary, String degree) {
            assert name != null;
            assert salary > 0;
            assert degree != null;

            this.name = name;
            this.salary = salary;
            this.degree = degree;
        }

        /** @return Id. */
        UUID id() {
            return id;
        }

        /** @return Name. */
        String name() {
            return name;
        }

        /** @return Salary. */
        double salary() {
            return salary;
        }

        /** @return Degree. */
        String degree() {
            return degree;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, id);
            U.writeString(out, name);
            out.writeInt(salary);
            U.writeString(out, degree);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = U.readUuid(in);
            name = U.readString(in);
            salary = in.readInt();
            degree = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode() + 31 * name.hashCode() + 31 * 31 * salary;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (!(obj instanceof PersonObj))
                return false;

            PersonObj that = (PersonObj)obj;

            return that.id.equals(id) && that.name.equals(name) && that.salary == salary && that.degree.equals(degree);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PersonObj.class, this);
        }
    }
}