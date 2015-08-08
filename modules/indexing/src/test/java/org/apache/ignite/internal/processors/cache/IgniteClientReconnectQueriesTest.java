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
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteClientReconnectQueriesTest extends IgniteClientReconnectAbstractTest {
    /** */
    public static final String QUERY_CACHE = "query";

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(QUERY_CACHE)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).getOrCreateCache(QUERY_CACHE).removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReconnect() throws Exception {
        Ignite cln = grid(serverCount());

        assertTrue(cln.cluster().localNode().isClient());

        final Ignite srv = clientRouter(cln);

        final IgniteCache<Integer, Person> clnCache = cln.getOrCreateCache(QUERY_CACHE);

        final IgniteCache<Integer, Person> srvCache = srv.getOrCreateCache(QUERY_CACHE);

        clnCache.put(1, new Person(1, "name1", "surname1"));
        clnCache.put(2, new Person(2, "name2", "surname2"));
        clnCache.put(3, new Person(3, "name3", "surname3"));

        final SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, "_key <> 0");

        qry.setPageSize(1);

        QueryCursor<Cache.Entry<Integer, Person>> cur = clnCache.query(qry);

        reconnectClientNode(cln, srv, new Runnable() {
            @Override public void run() {
                srvCache.put(4, new Person(4, "name4", "surname4"));

                try {
                    clnCache.query(qry);

                    fail();
                } catch (CacheException e) {
                    check(e);
                }
            }
        });

        List<Cache.Entry<Integer, Person>> res = cur.getAll();

        assertNotNull(res);
        assertEquals(4, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectQueryInProgress() throws Exception {
        Ignite cln = grid(serverCount());

        assertTrue(cln.cluster().localNode().isClient());

        final Ignite srv = clientRouter(cln);

        final IgniteCache<Integer, Person> clnCache = cln.getOrCreateCache(QUERY_CACHE);

        clnCache.put(1, new Person(1, "name1", "surname1"));
        clnCache.put(2, new Person(2, "name2", "surname2"));
        clnCache.put(3, new Person(3, "name3", "surname3"));

        blockMessage(GridQueryNextPageResponse.class);

        final SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, "_key <> 0");

        qry.setPageSize(1);

        final QueryCursor<Cache.Entry<Integer, Person>> cur1 = clnCache.query(qry);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    cur1.getAll();
                }
                catch (CacheException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        unblockMessage();

        reconnectClientNode(cln, srv, null);

        assertTrue((Boolean) fut.get(2, SECONDS));

        QueryCursor<Cache.Entry<Integer, Person>> cur2 = clnCache.query(qry);

        assertEquals(3, cur2.getAll().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryReconnect() throws Exception {
        Ignite cln = grid(serverCount());

        assertTrue(cln.cluster().localNode().isClient());

        final Ignite srv = clientRouter(cln);

        final IgniteCache<Integer, Person> clnCache = cln.getOrCreateCache(QUERY_CACHE);

        final IgniteCache<Integer, Person> srvCache = srv.getOrCreateCache(QUERY_CACHE);

        for (int i = 0; i < 10_000; i++)
            clnCache.put(i, new Person(i, "name-" + i, "surname-" + i));

        final ScanQuery<Integer, Person> scanQry = new ScanQuery<>();

        scanQry.setPageSize(1);

        scanQry.setFilter(new IgniteBiPredicate<Integer, Person>() {
            @Override public boolean apply(Integer integer, Person person) {
                return true;
            }
        });

        QueryCursor<Cache.Entry<Integer, Person>> qryCursor = clnCache.query(scanQry);

        reconnectClientNode(cln, srv, new Runnable() {
            @Override public void run() {
                srvCache.put(10_001, new Person(10_001, "name", "surname"));

                try {
                    clnCache.query(scanQry);

                    fail();
                } catch (CacheException e) {
                    check(e);
                }
            }
        });

        try {
            qryCursor.getAll();

            fail();
        }
        catch (CacheException e) {
            checkAndWait(e);
        }

        qryCursor = clnCache.query(scanQry);

        assertEquals(10_001, qryCursor.getAll().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryReconnectInProgress1() throws Exception {
        scanQueryReconnectInProgress(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryReconnectInProgress2() throws Exception {
        scanQueryReconnectInProgress(true);
    }

    /**
     * @param setPart If {@code true} sets partition for scan query.
     * @throws Exception If failed.
     */
    private void scanQueryReconnectInProgress(boolean setPart) throws Exception {
        Ignite cln = grid(serverCount());

        assertTrue(cln.cluster().localNode().isClient());

        final Ignite srv = clientRouter(cln);

        final IgniteCache<Integer, Person> clnCache = cln.getOrCreateCache(QUERY_CACHE);

        clnCache.put(1, new Person(1, "name1", "surname1"));
        clnCache.put(2, new Person(2, "name2", "surname2"));
        clnCache.put(3, new Person(3, "name3", "surname3"));

        final ScanQuery<Integer, Person> scanQry = new ScanQuery<>();

        scanQry.setPageSize(1);

        scanQry.setFilter(new IgniteBiPredicate<Integer, Person>() {
            @Override public boolean apply(Integer integer, Person person) {
                return true;
            }
        });

        if (setPart)
            scanQry.setPartition(1);

        blockMessage(GridCacheQueryResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    QueryCursor<Cache.Entry<Integer, Person>> qryCursor = clnCache.query(scanQry);

                    qryCursor.getAll();
                }
                catch (CacheException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        unblockMessage();

        reconnectClientNode(cln, srv, null);

        assertTrue((Boolean)fut.get(2, SECONDS));

        QueryCursor<Cache.Entry<Integer, Person>> qryCursor2 = clnCache.query(scanQry);

        assertEquals(setPart ? 1 : 3, qryCursor2.getAll().size());
    }

    /**
     * @param clazz Message class.
     */
    private void blockMessage(Class<?> clazz) {
        for (int i = 0; i < serverCount(); i++) {
            BlockTpcCommunicationSpi commSpi = commSpi(grid(i));

            commSpi.blockMessage(clazz);
        }
    }

    /**
     *
     */
    private void unblockMessage() {
        for (int i = 0; i < serverCount(); i++) {
            BlockTpcCommunicationSpi commSpi = commSpi(grid(i));

            commSpi.unblockMessage();
        }
    }

    /**
     *
     */
    public static class Person {
        /** */
        @QuerySqlField
        public int id;

        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField
        public String surname;

        /**
         * @param id Id.
         * @param name Name.
         * @param surname Surname.
         */
        public Person(int id, String name, String surname) {
            this.id = id;
            this.name = name;
            this.surname = surname;
        }

        /**
         * @return Id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Set id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return Surname.
         */
        public String getSurname() {
            return surname;
        }

        /**
         * @param surname Surname.
         */
        public void setSurname(String surname) {
            this.surname = surname;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass()) && id == ((Person)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
