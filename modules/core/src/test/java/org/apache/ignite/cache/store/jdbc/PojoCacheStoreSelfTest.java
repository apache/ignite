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
import org.apache.ignite.cache.store.jdbc.model.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.cache.GridAbstractCacheStoreSelfTest.*;
import org.gridgain.testframework.junits.common.*;
import org.h2.jdbcx.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
            metaUrl = new UrlResource(new File("modules/core/src/test/config/store/auto/all.xml").toURI().toURL());
        }
        catch (MalformedURLException e) {
            throw new IgniteCheckedException("Failed to resolve metadata path [err=" + e.getMessage() + ']', e);
        }

        try {
            GenericApplicationContext springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(metaUrl);

            springCtx.refresh();

            Collection<GridCacheQueryTypeMetadata> typeMetadata =
                springCtx.getBeansOfType(GridCacheQueryTypeMetadata.class).values();

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

        final Collection<OrganizationKey> orgKeys = new ArrayList<>(ORGANIZATION_CNT);
        final Collection<PersonKey> prnKeys = new ArrayList<>(PERSON_CNT);

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

        store.removeAll(null, orgKeys);
        store.removeAll(null, prnKeys);

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

        OrganizationKey k1 = new OrganizationKey(1);
        Organization v1 = new Organization(1, "Name1", "City1");

        OrganizationKey k2 = new OrganizationKey(2);
        Organization v2 = new Organization(2, "Name2", "City2");

        store.put(tx, k1, v1);
        store.put(tx, k2, v2);

        store.txEnd(tx, true);

        assertEquals(v1, store.load(null, k1));
        assertEquals(v2, store.load(null, k2));

        OrganizationKey k3 = new OrganizationKey(3);

        assertNull(store.load(tx, k3));

        store.remove(tx, k1);

        store.txEnd(tx, true);

        assertNull(store.load(tx, k1));
        assertEquals(v2, store.load(tx, k2));
        assertNull(store.load(null, k3));
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testRollback() throws IgniteCheckedException {
        IgniteTx tx = new DummyTx();

        OrganizationKey k1 = new OrganizationKey(1);
        Organization v1 = new Organization(1, "Name1", "City1");

        // Put.
        store.put(tx, k1, v1);

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertNull(store.load(tx, k1));

        OrganizationKey k2 = new OrganizationKey(2);
        Organization v2 = new Organization(2, "Name2", "City2");

        // Put all.
        assertNull(store.load(tx, k2));

        store.putAll(tx, Collections.singletonMap(k2, v2));

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertNull(store.load(tx, k2));

        OrganizationKey k3 = new OrganizationKey(3);
        Organization v3 = new Organization(3, "Name3", "City3");

        store.putAll(tx, Collections.singletonMap(k3, v3));

        store.txEnd(tx, true); // Commit.

        tx = new DummyTx();

        assertEquals(v3, store.load(tx, k3));

        OrganizationKey k4 = new OrganizationKey(4);
        Organization v4 = new Organization(4, "Name4", "City4");

        store.put(tx, k4, v4);

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertNull(store.load(tx, k4));

        assertEquals(v3, store.load(tx, k3));

        // Remove.
        store.remove(tx, k3);

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertEquals(v3, store.load(tx, k3));

        // Remove all.
        store.removeAll(tx, Arrays.asList(k3));

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertEquals(v3, store.load(tx, k3));
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testAllOpsWithTXNoCommit() throws IgniteCheckedException {
        doTestAllOps(new DummyTx(), false);
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testAllOpsWithTXCommit() throws IgniteCheckedException {
        doTestAllOps(new DummyTx(), true);
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testAllOpsWithoutTX() throws IgniteCheckedException {
        doTestAllOps(null, false);
    }

    /**
     * @param tx Transaction.
     * @param commit Commit.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestAllOps(@Nullable IgniteTx tx, boolean commit) throws IgniteCheckedException {
        try {
            final OrganizationKey k1 = new OrganizationKey(1);
            final Organization v1 = new Organization(1, "Name1", "City1");

            store.put(tx, k1, v1);

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            if (tx == null || commit)
                assertEquals(v1, store.load(tx, k1));

            Map<OrganizationKey, Organization> m = new HashMap<>();

            final OrganizationKey k2 = new OrganizationKey(2);
            final Organization v2 = new Organization(2, "Name2", "City2");

            final OrganizationKey k3 = new OrganizationKey(3);
            final Organization v3 = new Organization(3, "Name3", "City3");

            m.put(k2, v2);
            m.put(k3, v3);

            store.putAll(tx, m);

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            final AtomicInteger cntr = new AtomicInteger();

            final OrganizationKey no_such_key = new OrganizationKey(4);

            if (tx == null || commit) {
                store.loadAll(tx, Arrays.asList(k1, k2, k3, no_such_key), new CI2<Object, Object>() {
                    @Override public void apply(Object o, Object o1) {
                        if (k1.equals(o))
                            assertEquals(v1, o1);

                        if (k2.equals(o))
                            assertEquals(v2, o1);

                        if (k3.equals(o))
                            assertEquals(v3, o1);

                        if (no_such_key.equals(o))
                            fail();

                        cntr.incrementAndGet();
                    }
                });

                assertEquals(3, cntr.get());
            }

            store.removeAll(tx, Arrays.asList(k2, k3));

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            if (tx == null || commit) {
                assertNull(store.load(tx, k2));
                assertNull(store.load(tx, k3));
                assertEquals(v1, store.load(tx, k1));
            }

            store.remove(tx, k1);

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            if (tx == null || commit)
                assertNull(store.load(tx, k1));
        }
        finally {
            if (tx != null)
                store.txEnd(tx, false);
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

                    int op = rnd.nextInt(10);

                    boolean queueEmpty = false;

                    if (op < 4) { // Load.
                        OrganizationKey key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                assertNotNull(store.load(tx, key));
                            else {
                                final AtomicInteger cntr = new AtomicInteger();

                                store.loadAll(tx, Collections.singleton(key), new CI2<Object, Object>() {
                                    @Override public void apply(Object o, Object o1) {
                                        cntr.incrementAndGet();

                                        assertNotNull(o);
                                        assertNotNull(o1);

                                        OrganizationKey key = (OrganizationKey)o;
                                        Organization val = (Organization)o1;

                                        assertTrue(key.getId().equals(val.getId()));
                                    }
                                });

                                assertEquals(1, cntr.get());
                            }

                            if (tx != null)
                                store.txEnd(tx, true);

                            queue.add(key);
                        }
                    }
                    else if (op < 6) { // Remove.
                        OrganizationKey key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                store.remove(tx, key);
                            else
                                store.removeAll(tx, Collections.singleton(key));

                            if (tx != null)
                                store.txEnd(tx, true);
                        }
                    }
                    else { // Update.
                        OrganizationKey key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            Organization val =
                                new Organization(key.getId(), "Name" + key.getId(), "City" + key.getId());

                            if (rnd.nextBoolean())
                                store.put(tx, key, val);
                            else
                                store.putAll(tx, Collections.singletonMap(key, val));

                            if (tx != null)
                                store.txEnd(tx, true);

                            queue.add(key);
                        }
                    }

                    if (queueEmpty) { // Add.
                        OrganizationKey key = new OrganizationKey(rnd.nextInt());
                        Organization val = new Organization(key.getId(), "Name" + key.getId(), "City" + key.getId());

                        if (rnd.nextBoolean())
                            store.put(tx, key, val);
                        else
                            store.putAll(tx, Collections.singletonMap(key, val));

                        if (tx != null)
                            store.txEnd(tx, true);

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
