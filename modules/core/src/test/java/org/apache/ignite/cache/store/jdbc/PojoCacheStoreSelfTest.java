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
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.model.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.cache.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.h2.jdbcx.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import javax.cache.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.testframework.junits.cache.GridAbstractCacheStoreSelfTest.*;

/**
 * Class for {@code PojoCacheStore} tests.
 */
public class PojoCacheStoreSelfTest extends GridCommonAbstractTest {
    /** Default connection URL (value is <tt>jdbc:h2:mem:jdbcCacheStore;DB_CLOSE_DELAY=-1</tt>). */
    protected static final String DFLT_CONN_URL = "jdbc:h2:mem:autoCacheStore;DB_CLOSE_DELAY=-1";

    /** Organization count. */
    protected static final int ORGANIZATION_CNT = 1000;

    /** Person count. */
    protected static final int PERSON_CNT = 100000;

    /** */
    protected TestThreadLocalCacheSession ses = new TestThreadLocalCacheSession();

    /** */
    protected final JdbcPojoCacheStore store;

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    public PojoCacheStoreSelfTest() throws Exception {
        super(false);

        store = store();

        inject(store);
    }

    /**
     * @return Store.
     */
    protected JdbcPojoCacheStore store() throws IgniteCheckedException {
        JdbcPojoCacheStore store = new JdbcPojoCacheStore();

        store.setDataSource(JdbcConnectionPool.create(DFLT_CONN_URL, "sa", ""));

        UrlResource metaUrl;

        try {
            metaUrl = new UrlResource(new File("modules/core/src/test/config/store/jdbc/Ignite.xml").toURI().toURL());
        }
        catch (MalformedURLException e) {
            throw new IgniteCheckedException("Failed to resolve metadata path [err=" + e.getMessage() + ']', e);
        }

        try {
            GenericApplicationContext springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(metaUrl);

            springCtx.refresh();

            Collection<CacheQueryTypeMetadata> typeMetadata =
                springCtx.getBeansOfType(CacheQueryTypeMetadata.class).values();

            store.setTypeMetadata(typeMetadata);
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + metaUrl + ']', e);
            else
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context [springUrl=" +
                    metaUrl + ", err=" + e.getMessage() + ']', e);
        }

        return store;
    }

    /**
     * @param store Store.
     * @throws Exception If failed.
     */
    protected void inject(JdbcCacheStore store) throws Exception {
        getTestResources().inject(store);

        GridTestUtils.setFieldValue(store, CacheStore.class, "ses", ses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "sa", "");

        Statement stmt = conn.createStatement();

        PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO Organization(id, name, city) VALUES (?, ?, ?)");

        for (int i = 0; i < ORGANIZATION_CNT; i++) {
            orgStmt.setInt(1, i);
            orgStmt.setString(2, "name" + i);
            orgStmt.setString(3, "city" + i % 10);

            orgStmt.addBatch();
        }

        orgStmt.executeBatch();

        conn.commit();

        PreparedStatement prnStmt = conn.prepareStatement("INSERT INTO Person(id, org_id, name) VALUES (?, ?, ?)");

        for (int i = 0; i < PERSON_CNT; i++) {
            prnStmt.setInt(1, i);
            prnStmt.setInt(2, i % 100);
            prnStmt.setString(3, "name" + i);

            prnStmt.addBatch();
        }

        prnStmt.executeBatch();

        conn.commit();

        U.closeQuiet(stmt);

        U.closeQuiet(conn);

        final Collection<OrganizationKey> orgKeys = new ConcurrentLinkedQueue<>();
        final Collection<PersonKey> prnKeys = new ConcurrentLinkedQueue<>();

        IgniteBiInClosure<Object, Object> c = new CI2<Object, Object>() {
            @Override public void apply(Object k, Object v) {
                if (k instanceof OrganizationKey && v instanceof Organization)
                    orgKeys.add((OrganizationKey)k);
                else if (k instanceof PersonKey && v instanceof Person)
                    prnKeys.add((PersonKey)k);
            }
        };

        store.loadCache(c);

        assertEquals(ORGANIZATION_CNT, orgKeys.size());
        assertEquals(PERSON_CNT, prnKeys.size());

        store.deleteAll(orgKeys);
        store.deleteAll(prnKeys);

        orgKeys.clear();
        prnKeys.clear();

        store.loadCache(c);

        assertTrue(orgKeys.isEmpty());
        assertTrue(prnKeys.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testStore() throws Exception {
        // Create dummy transaction
        IgniteTx tx = new DummyTx();

        ses.newSession(tx);

        OrganizationKey k1 = new OrganizationKey(1);
        Organization v1 = new Organization(1, "Name1", "City1");

        OrganizationKey k2 = new OrganizationKey(2);
        Organization v2 = new Organization(2, "Name2", "City2");

        store.write(new CacheEntryImpl<>(k1, v1));
        store.write(new CacheEntryImpl<>(k2, v2));

        store.txEnd(true);

        ses.newSession(null);

        assertEquals(v1, store.load(k1));
        assertEquals(v2, store.load(k2));

        ses.newSession(tx);

        OrganizationKey k3 = new OrganizationKey(3);

        assertNull(store.load(k3));

        store.delete(k1);

        store.txEnd(true);

        assertNull(store.load(k1));
        assertEquals(v2, store.load(k2));

        ses.newSession(null);

        assertNull(store.load(k3));
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testRollback() throws IgniteCheckedException {
        IgniteTx tx = new DummyTx();

        ses.newSession(tx);

        OrganizationKey k1 = new OrganizationKey(1);
        Organization v1 = new Organization(1, "Name1", "City1");

        // Put.
        store.write(new CacheEntryImpl<>(k1, v1));

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load(k1));

        OrganizationKey k2 = new OrganizationKey(2);
        Organization v2 = new Organization(2, "Name2", "City2");

        // Put all.
        assertNull(store.load(k2));

        Collection<Cache.Entry<?, ?>> col = new ArrayList<>();

        col.add(new CacheEntryImpl<>(k2, v2));

        store.writeAll(col);

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load(k2));

        OrganizationKey k3 = new OrganizationKey(3);
        Organization v3 = new Organization(3, "Name3", "City3");

        col = new ArrayList<>();

        col.add(new CacheEntryImpl<>(k3, v3));

        store.writeAll(col);

        store.txEnd(true); // Commit.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals(v3, store.load(k3));

        OrganizationKey k4 = new OrganizationKey(4);
        Organization v4 = new Organization(4, "Name4", "City4");

        store.write(new CacheEntryImpl<>(k4, v4));

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load(k4));

        assertEquals(v3, store.load(k3));

        // Remove.
        store.delete(k3);

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals(v3, store.load(k3));

        // Remove all.
        store.deleteAll(Arrays.asList(k3));

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals(v3, store.load(k3));
    }

    /**
     */
    public void testAllOpsWithTXNoCommit() {
        doTestAllOps(new DummyTx(), false);
    }

    /**
     */
    public void testAllOpsWithTXCommit() {
        doTestAllOps(new DummyTx(), true);
    }

    /**
     */
    public void testAllOpsWithoutTX() {
        doTestAllOps(null, false);
    }

    /**
     * @param tx Transaction.
     * @param commit Commit.
     */
    private void doTestAllOps(@Nullable IgniteTx tx, boolean commit) {
        try {
            ses.newSession(tx);

            final OrganizationKey k1 = new OrganizationKey(1);
            final Organization v1 = new Organization(1, "Name1", "City1");

            store.write(new CacheEntryImpl<>(k1, v1));

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit)
                assertEquals(v1, store.load(k1));

            Collection<Cache.Entry<?, ?>> col = new ArrayList<>();

            final OrganizationKey k2 = new OrganizationKey(2);
            final Organization v2 = new Organization(2, "Name2", "City2");

            final OrganizationKey k3 = new OrganizationKey(3);
            final Organization v3 = new Organization(3, "Name3", "City3");

            col.add(new CacheEntryImpl<>(k2, v2));
            col.add(new CacheEntryImpl<>(k3, v3));

            store.writeAll(col);

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            final AtomicInteger cntr = new AtomicInteger();

            final OrganizationKey no_such_key = new OrganizationKey(4);

            if (tx == null || commit) {
                Map<Object, Object> loaded = store.loadAll(Arrays.asList(k1, k2, k3, no_such_key));

                for (Map.Entry<Object, Object> e : loaded.entrySet()) {
                    Object key = e.getKey();
                    Object val = e.getValue();

                    if (k1.equals(key))
                        assertEquals(v1, val);

                    if (k2.equals(key))
                        assertEquals(v2, val);

                    if (k3.equals(key))
                        assertEquals(v3, val);

                    if (no_such_key.equals(key))
                        fail();

                    cntr.incrementAndGet();
                }

                assertEquals(3, cntr.get());
            }

            store.deleteAll(Arrays.asList(k2, k3));

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit) {
                assertNull(store.load(k2));
                assertNull(store.load(k3));
                assertEquals(v1, store.load(k1));
            }

            store.delete(k1);

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit)
                assertNull(store.load(k1));
        }
        finally {
            if (tx != null)
                store.txEnd(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleMultithreading() throws Exception {
        final Random rnd = new Random();

        final Queue<OrganizationKey> queue = new LinkedBlockingQueue<>();

        multithreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                for (int i = 0; i < 1000; i++) {
                    IgniteTx tx = rnd.nextBoolean() ? new DummyTx() : null;

                    ses.newSession(tx);

                    int op = rnd.nextInt(10);

                    boolean queueEmpty = false;

                    if (op < 4) { // Load.
                        OrganizationKey key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                assertNotNull(store.load(key));
                            else {
                                Map<Object, Object> loaded = store.loadAll(Collections.singleton(key));

                                assertEquals(1, loaded.size());

                                Map.Entry<Object, Object> e = loaded.entrySet().iterator().next();

                                OrganizationKey k = (OrganizationKey)e.getKey();
                                Organization v = (Organization)e.getValue();

                                assertTrue(k.getId().equals(v.getId()));
                            }

                            if (tx != null)
                                store.txEnd(true);

                            queue.add(key);
                        }
                    }
                    else if (op < 6) { // Remove.
                        OrganizationKey key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                store.delete(key);
                            else
                                store.deleteAll(Collections.singleton(key));

                            if (tx != null)
                                store.txEnd(true);
                        }
                    }
                    else { // Update.
                        OrganizationKey key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            Organization val =
                                new Organization(key.getId(), "Name" + key.getId(), "City" + key.getId());

                            Cache.Entry<OrganizationKey, Organization> entry = new CacheEntryImpl<>(key, val);

                            if (rnd.nextBoolean())
                                store.write(entry);
                            else {
                                Collection<Cache.Entry<?, ?>> col = new ArrayList<>();

                                col.add(entry);

                                store.writeAll(col);
                            }

                            if (tx != null)
                                store.txEnd(true);

                            queue.add(key);
                        }
                    }

                    if (queueEmpty) { // Add.
                        OrganizationKey key = new OrganizationKey(rnd.nextInt());
                        Organization val = new Organization(key.getId(), "Name" + key.getId(), "City" + key.getId());

                        Cache.Entry<OrganizationKey, Organization> entry = new CacheEntryImpl<>(key, val);

                        if (rnd.nextBoolean())
                            store.write(entry);
                        else {
                            Collection<Cache.Entry<?, ?>> col = new ArrayList<>();

                            col.add(entry);

                            store.writeAll(col);
                        }

                        if (tx != null)
                            store.txEnd(true);

                        queue.add(key);
                    }
                }

                return null;
            }
        }, 37);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

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
}
