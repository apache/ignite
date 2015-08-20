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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.jdbc.model.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.testframework.GridTestUtils.*;

/**
 *
 */
public abstract class CacheJdbcStoreAbstractMultithreadedSelfTest<T extends CacheAbstractJdbcStore>
    extends GridCommonAbstractTest {
    /** Default config with mapping. */
    private static final String DFLT_MAPPING_CONFIG = "modules/core/src/test/config/store/jdbc/ignite-type-metadata.xml";

    /** Database connection URL. */
    protected static final String DFLT_CONN_URL = "jdbc:h2:mem:autoCacheStore;DB_CLOSE_DELAY=-1";

    /** IP finder. */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of transactions. */
    private static final int TX_CNT = 200;

    /** Number of transactions. */
    private static final int BATCH_CNT = 2000;

    /** Cache store. */
    protected static CacheAbstractJdbcStore store;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        store = store();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        store = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "sa", "");

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS Organization");
        stmt.executeUpdate("DROP TABLE IF EXISTS Person");

        stmt.executeUpdate("CREATE TABLE Organization (id integer PRIMARY KEY, name varchar(50), city varchar(50))");
        stmt.executeUpdate("CREATE TABLE Person (id integer PRIMARY KEY, org_id integer, name varchar(50))");

        conn.commit();

        U.closeQuiet(stmt);

        U.closeQuiet(conn);

        startGrid();
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setCacheConfiguration(cacheConfiguration());

        return c;
    }

    /** */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(ATOMIC);
        cc.setSwapEnabled(false);
        cc.setWriteBehindEnabled(false);

        URL cfgUrl;

        try {
            cfgUrl = new URL(DFLT_MAPPING_CONFIG);
        }
        catch (MalformedURLException ignore) {
            cfgUrl = U.resolveIgniteUrl(DFLT_MAPPING_CONFIG);
        }

        if (cfgUrl == null)
            throw new Exception("Failed to resolve metadata path: " + DFLT_MAPPING_CONFIG);

        try {
            GenericApplicationContext springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(cfgUrl));

            springCtx.refresh();

            Collection<CacheTypeMetadata> tp = new ArrayList<>(springCtx.getBeansOfType(CacheTypeMetadata.class).values());

            cc.setTypeMetadata(tp);
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + cfgUrl + ']', e);
            else
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context [springUrl=" +
                    cfgUrl + ", err=" + e.getMessage() + ']', e);
        }

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPut() throws Exception {
        IgniteInternalFuture<?> fut1 = runMultiThreadedAsync(new Callable<Object>() {
            private final Random rnd = new Random();

            @Nullable @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    IgniteCache<Object, Object> cache = jcache();

                    int id = rnd.nextInt(1000);

                    if (rnd.nextBoolean())
                        cache.put(new OrganizationKey(id), new Organization(id, "Name" + id, "City" + id));
                    else
                        cache.put(new PersonKey(id), new Person(id, rnd.nextInt(), "Name" + id, 1));
                }

                return null;
            }
        }, 4, "put");

        IgniteInternalFuture<?> fut2 = runMultiThreadedAsync(new Callable<Object>() {
            private final Random rnd = new Random();

            @Nullable @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    IgniteCache<Object, Object> cache = jcache();

                    int id = rnd.nextInt(1000);

                    if (rnd.nextBoolean())
                        cache.putIfAbsent(new OrganizationKey(id), new Organization(id, "Name" + id, "City" + id));
                    else
                        cache.putIfAbsent(new PersonKey(id), new Person(id, rnd.nextInt(), "Name" + id, i));
                }

                return null;
            }
        }, 8, "putIfAbsent");

        fut1.get();
        fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPutAll() throws Exception {
        multithreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @Nullable @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    int cnt = rnd.nextInt(BATCH_CNT);

                    List<Integer> ids = new ArrayList<>(cnt);

                    for (int j = 0; j < cnt; j++) {
                        int id = rnd.nextInt(5000);

                        if (!ids.contains(id))
                            ids.add(id);
                    }

                    Collections.sort(ids);

                    Map<Object, Object> map = U.newLinkedHashMap(cnt);

                    for (Integer id : ids) {
                        if (rnd.nextBoolean())
                            map.put(new OrganizationKey(id), new Organization(id, "Name" + id, "City" + id));
                        else
                            map.put(new PersonKey(id), new Person(id, rnd.nextInt(), "Name" + id, 1));
                    }

                    IgniteCache<Object, Object> cache = jcache();

                    cache.putAll(map);
                }

                return null;
            }
        }, 8, "putAll");
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedExplicitTx() throws Exception {
        runMultiThreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @Nullable @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    IgniteCache<PersonKey, Person> cache = jcache();

                    try (Transaction tx = grid().transactions().txStart()) {
                        cache.put(new PersonKey(1), new Person(1, rnd.nextInt(), "Name" + 1, 1));
                        cache.put(new PersonKey(2), new Person(2, rnd.nextInt(), "Name" + 2, 2));
                        cache.put(new PersonKey(3), new Person(3, rnd.nextInt(), "Name" + 3, 3));

                        cache.get(new PersonKey(1));
                        cache.get(new PersonKey(4));

                        Map<PersonKey, Person> map =  U.newHashMap(2);

                        map.put(new PersonKey(5), new Person(5, rnd.nextInt(), "Name" + 5, 5));
                        map.put(new PersonKey(6), new Person(6, rnd.nextInt(), "Name" + 6, 6));

                        cache.putAll(map);

                        tx.commit();
                    }
                }

                return null;
            }
        }, 8, "tx");
    }
}
