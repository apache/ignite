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

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.jdbc.common.TestPogo;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CacheJdbcPojoStore} created via XML.
 */
@RunWith(JUnit4.class)
public class CacheJdbcPojoWriteBehindHangStoreWithCoalescingTest extends GridCommonAbstractTest {
    /** */
    private static final String DFLT_CONN_URL = "jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1";

    /** */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        ignite = Ignition.start("modules/spring/src/test/config/jdbc-write-behind-coalescing.xml");

        ignite.cluster().active(true);

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        Ignition.stop(ignite.configuration().getIgniteInstanceName(), true);

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonCoalescingIncrementing() throws Exception {
        try {
            Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "sa", "");

            Statement stmt = conn.createStatement();

            stmt.executeUpdate("DROP TABLE IF EXISTS TEST_CACHE");

            stmt.executeUpdate("CREATE TABLE TEST_CACHE (" +
                " VALUE2 INTEGER PRIMARY KEY," +
                " VALUE1 VARCHAR(50)," +
                " VALUE3 DATE" +
                ")"
            );

            conn.commit();

            U.closeQuiet(stmt);

            U.closeQuiet(conn);
        } catch (SQLException ex) {

        }

        IgniteCache<Integer, TestPogo> cache = ignite.cache("TEST_CACHE");

        AtomicInteger t1Count = new AtomicInteger(10);
        AtomicInteger t2Count = new AtomicInteger(10);

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t1Count.get() > 0) {
                        for (int i = 0; i < 5000; i++)
                            cache.put(i, new TestPogo("ORIGIN" + i, i, new java.sql.Date(new java.util.Date().getTime())));

                        t1Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t2Count.get() > 0) {
                        for (int i = 0; i < 5000; i++)
                            cache.put(i, new TestPogo("UPDATE" + i, i, new java.sql.Date(new java.util.Date().getTime())));

                        try {
                            U.sleep(500);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }

                        t2Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        t1.start();

        t2.start();

        //every write to cache store will take more then 10 seconds. t1 should be completed before 10 seconds.
        U.sleep(10000);

        assertEquals(0, t1Count.get());
    }
}

