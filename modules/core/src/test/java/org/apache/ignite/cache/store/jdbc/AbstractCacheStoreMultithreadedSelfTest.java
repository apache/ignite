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

import org.apache.ignite.cache.store.jdbc.model.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.testframework.GridTestUtils.*;

/**
 *
 */
public abstract class AbstractCacheStoreMultithreadedSelfTest<T extends JdbcCacheStore> extends GridCommonAbstractTest {
    /** Default connection URL (value is <tt>jdbc:h2:mem:jdbcCacheStore;DB_CLOSE_DELAY=-1</tt>). */
    protected static final String DFLT_CONN_URL = "jdbc:h2:mem:autoCacheStore;DB_CLOSE_DELAY=-1";

    /** IP finder. */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of transactions. */
    private static final int TX_CNT = 1000;

    /** Number of transactions. */
    private static final int BATCH_CNT = 2000;

    /** Cache store. */
    protected T store;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        store = store();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "sa", "");

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS Organization");
        stmt.executeUpdate("DROP TABLE IF EXISTS Person");

        stmt.executeUpdate("CREATE TABLE Organization (id integer PRIMARY KEY, name varchar(50), city varchar(50))");
        stmt.executeUpdate("CREATE TABLE Person (id integer PRIMARY KEY, org_id integer, name varchar(50))");

        stmt.executeUpdate("CREATE INDEX Org_Name_IDX On Organization (name)");
        stmt.executeUpdate("CREATE INDEX Org_Name_City_IDX On Organization (name, city)");
        stmt.executeUpdate("CREATE INDEX Person_Name_IDX1 On Person (name)");
        stmt.executeUpdate("CREATE INDEX Person_Name_IDX2 On Person (name desc)");

        conn.commit();

        U.closeQuiet(stmt);

        U.closeQuiet(conn);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @return New store.
     * @throws Exception In case of error.
     */
    protected abstract T store() throws Exception;

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPutAll() throws Exception {
        startGrid();

        runMultiThreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @Nullable @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    int cnt = rnd.nextInt(BATCH_CNT);

                    Map<Object, Object> map = U.newHashMap(cnt);

                    for (int j = 0; j < cnt; j++) {
                        int id = rnd.nextInt();

                        if (rnd.nextBoolean())
                            map.put(new OrganizationKey(id), new Organization(id, "Name" + id, "City" + id));
                        else
                            map.put(new PersonKey(id), new Person(id, rnd.nextInt(), "Name" + id));
                    }

                    GridCache<Object, Object> cache = cache();

                    cache.putAll(map);
                }

                return null;
            }
        }, 8, "putAll");
    }
}
