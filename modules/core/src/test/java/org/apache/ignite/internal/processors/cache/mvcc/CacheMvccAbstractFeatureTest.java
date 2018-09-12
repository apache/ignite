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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test that checks
 */
public abstract class CacheMvccAbstractFeatureTest extends CacheMvccAbstractTest {
    /** */
    private static final String CACHE_NAME = "Person";

    /** */
    private Ignite node;

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(4);

        node = grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setIndexedTypes(Integer.class, Person.class);

        node.createCache(ccfg);

        for (int i = 0; i < 100; i++)
            cache().put(i, new Person("Name" + i, "LastName" + i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node.destroyCache(CACHE_NAME);
    }

    /**
     * @param clo Closure to check consistency upon.
     * @throws Exception if failed.
     */
    void doTestConsistency(IgniteClosure2X<CountDownLatch, CountDownLatch, ?> clo) throws Exception {
        ExecutorService svc = Executors.newFixedThreadPool(2);

        CountDownLatch startLatch = new CountDownLatch(1);

        CountDownLatch endLatch = new CountDownLatch(1);

        try {
            Future<IgnitePair<?>> fut = svc.submit(new Callable<IgnitePair<?>>() {
                @Override public IgnitePair<?> call() {
                    try (Transaction ignored = node.transactions().txStart()) {
                        // First result that we'll later check w/respect to REPEATABLE READ semantic.
                        Object res1 = clo.apply(null, null);

                        Object res2 = clo.apply(startLatch, endLatch);

                        return new IgnitePair<>(res1, res2);
                    }
                }
            });

            svc.submit(new Runnable() {
                @Override public void run() {
                    try {
                        startLatch.await();
                    }
                    catch (InterruptedException e) {
                        throw new IgniteInterruptedException(e);
                    }

                    try {
                        modifyData(jdbcTx());
                    }
                    catch (SQLException e) {
                        throw new IgniteException(e);
                    }

                    endLatch.countDown();
                }
            }).get();

            IgnitePair<?> res2 = fut.get();

            assertEquals(res2.get1(), res2.get2());
        }
        finally {
            svc.shutdown();
        }
    }

    /**
     * @return Whether native or SQL transactions must be used.
     */
    boolean jdbcTx() {
        return false;
    }

    /**
     * @param jdbcTx Whether concurrent transaction must be of SQL type.
     */
    private void modifyData(boolean jdbcTx) throws SQLException {
        Set<Integer> keys = new HashSet<>(10);

        for (int i = 0; i < 10; i++) {
            int idx;

            do {
                idx = (int) (Math.random() * 100) + 1;
            }
            while (!keys.add(idx));
        }

        if (!jdbcTx) {
            try (Transaction ignored = node.transactions().txStart()) {
                for (int idx : keys) {
                    boolean rmv = Math.random() > 0.5;

                    if (rmv)
                        cache().remove(idx);
                    else {
                        Person p = cache().get(idx);

                        cache().put(idx, new Person(p.fName, p.fName + "Updated"));
                    }
                }
            }
        }
        else {
            try (Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
                c.setAutoCommit(false);

                for (int idx : keys) {
                    boolean rmv = Math.random() > 0.5;

                    if (rmv) {
                        try (Statement s = c.createStatement()) {
                            s.execute("DELETE FROM \"Person\".PERSON WHERE _key = " + idx);
                        }
                    }
                    else {
                        try (Statement s = c.createStatement()) {
                            s.execute("UPDATE \"Person\".PERSON SET lname = concat(lname, 'Updated')" +
                                "WHERE _key = " + idx);
                        }
                    }
                }

                try (Statement s = c.createStatement()) {
                    s.execute("COMMIT");
                }
            }
        }
    }

    /**
     * @return Cache.
     */
    IgniteCache<Integer, Person> cache() {
        return node.cache(CACHE_NAME);
    }

    /**
     *
     */
    static class Person implements Serializable {
        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String fName;

        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String lName;

        /**
         * @param fName First name.
         * @param lName Last name.
         */
        public Person(String fName, String lName) {
            this.fName = fName;
            this.lName = lName;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(fName, person.fName) &&
                Objects.equals(lName, person.lName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(fName, lName);
        }
    }

    /** */
    final static Comparator<Cache.Entry<Integer, Person>> ENTRY_CMP =
        new Comparator<Cache.Entry<Integer, Person>>() {
        @Override public int compare(Cache.Entry<Integer, Person> o1, Cache.Entry<Integer, Person> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    /**
     *
     */
    static List<Person> entriesToPersons(List<Cache.Entry<Integer, Person>> entries) {
        entries.sort(ENTRY_CMP);

        List<Person> res = new ArrayList<>();

        for (Cache.Entry<Integer, Person> e : entries)
            res.add(e.getValue());

        return res;
    }
}
