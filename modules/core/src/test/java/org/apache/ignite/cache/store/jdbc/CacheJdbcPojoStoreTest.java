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
import org.apache.ignite.cache.store.jdbc.dialect.*;
import org.apache.ignite.cache.store.jdbc.model.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.cache.*;
import org.h2.jdbcx.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import javax.cache.integration.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Class for {@code PojoCacheStore} tests.
 */
public class CacheJdbcPojoStoreTest extends GridAbstractCacheStoreSelfTest<CacheJdbcPojoStore<Object, Object>> {
    /** DB connection URL. */
    private static final String DFLT_CONN_URL = "jdbc:h2:mem:autoCacheStore;DB_CLOSE_DELAY=-1";

    /** Default config with mapping. */
    private static final String DFLT_MAPPING_CONFIG = "modules/core/src/test/config/store/jdbc/ignite-type-metadata.xml";

    /** Organization count. */
    protected static final int ORGANIZATION_CNT = 1000;

    /** Person count. */
    protected static final int PERSON_CNT = 100000;

    /**
     * @throws Exception If failed.
     */
    public CacheJdbcPojoStoreTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected CacheJdbcPojoStore<Object, Object> store() {
        CacheJdbcPojoStore<Object, Object> store = new CacheJdbcPojoStore<>();

//        PGPoolingDataSource ds = new PGPoolingDataSource();
//        ds.setUser("postgres");
//        ds.setPassword("postgres");
//        ds.setServerName("ip");
//        ds.setDatabaseName("postgres");
//        store.setDataSource(ds);

//        MysqlDataSource ds = new MysqlDataSource();
//        ds.setURL("jdbc:mysql://ip:port/dbname");
//        ds.setUser("mysql");
//        ds.setPassword("mysql");

        store.setDataSource(JdbcConnectionPool.create(DFLT_CONN_URL, "sa", ""));

        URL cfgUrl;

        try {
            cfgUrl = new URL(DFLT_MAPPING_CONFIG);
        }
        catch (MalformedURLException ignore) {
            cfgUrl = U.resolveIgniteUrl(DFLT_MAPPING_CONFIG);
        }

        if (cfgUrl == null)
            throw new IgniteException("Failed to resolve metadata path: " + DFLT_MAPPING_CONFIG);

        try {
            GenericApplicationContext springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(cfgUrl));

            springCtx.refresh();

            Collection<CacheTypeMetadata> typeMeta = springCtx.getBeansOfType(CacheTypeMetadata.class).values();

            Map<Integer, Map<Object, CacheAbstractJdbcStore.EntryMapping>> cacheMappings = new HashMap<>();

            JdbcDialect dialect = store.resolveDialect();

            GridTestUtils.setFieldValue(store, CacheAbstractJdbcStore.class, "dialect", dialect);

            Map<Object, CacheAbstractJdbcStore.EntryMapping> entryMappings = U.newHashMap(typeMeta.size());

            for (CacheTypeMetadata type : typeMeta)
                entryMappings.put(store.keyTypeId(type.getKeyType()),
                    new CacheAbstractJdbcStore.EntryMapping(null, dialect, type));

            store.prepareBuilders(null, typeMeta);

            cacheMappings.put(null, entryMappings);

            GridTestUtils.setFieldValue(store, CacheAbstractJdbcStore.class, "cacheMappings", cacheMappings);
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new IgniteException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + cfgUrl + ']', e);
            else
                throw new IgniteException("Failed to instantiate Spring XML application context [springUrl=" +
                    cfgUrl + ", err=" + e.getMessage() + ']', e);
        }

        return store;
    }

    /**
     * @param store Store.
     * @throws Exception If failed.
     */
    @Override protected void inject(CacheJdbcPojoStore<Object, Object> store) throws Exception {
        getTestResources().inject(store);

        GridTestUtils.setFieldValue(store, CacheAbstractJdbcStore.class, "ses", ses);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = store.openConnection(false);

        Statement stmt = conn.createStatement();

        try {
            stmt.executeUpdate("delete from String_Entries");
        }
        catch (SQLException ignore) {
            // No-op.
        }

        try {
            stmt.executeUpdate("delete from UUID_Entries");
        }
        catch (SQLException ignore) {
            // No-op.
        }

        try {
            stmt.executeUpdate("delete from Organization");
        }
        catch (SQLException ignore) {
            // No-op.
        }

        try {
            stmt.executeUpdate("delete from Person");
        }
        catch (SQLException ignore) {
            // No-op.
        }

        try {
            stmt.executeUpdate("delete from Timestamp_Entries");
        }
        catch (SQLException ignore) {
            // No-op.
        }

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS String_Entries (key varchar(100) not null, val varchar(100), PRIMARY KEY(key))");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS UUID_Entries (key binary(16) not null, val binary(16), PRIMARY KEY(key))");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Timestamp_Entries (key timestamp not null, val integer, PRIMARY KEY(key))");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Organization (id integer not null, name varchar(50), city varchar(50), PRIMARY KEY(id))");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Person (id integer not null, org_id integer, name varchar(50), PRIMARY KEY(id))");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Person_Complex (id integer not null, org_id integer not null, city_id integer not null, name varchar(50), PRIMARY KEY(id))");

        conn.commit();

        U.closeQuiet(stmt);

        U.closeQuiet(conn);

        super.beforeTest();
    }


    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        Connection conn = store.openConnection(false);

        PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO Organization(id, name, city) VALUES (?, ?, ?)");

        for (int i = 0; i < ORGANIZATION_CNT; i++) {
            orgStmt.setInt(1, i);
            orgStmt.setString(2, "name" + i);
            orgStmt.setString(3, "city" + i % 10);

            orgStmt.addBatch();
        }

        orgStmt.executeBatch();

        U.closeQuiet(orgStmt);

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

        U.closeQuiet(prnStmt);

        PreparedStatement prnComplexStmt = conn.prepareStatement("INSERT INTO Person_Complex(id, org_id, city_id, name) VALUES (?, ?, ?, ?)");

        for (int i = 0; i < PERSON_CNT; i++) {
            prnComplexStmt.setInt(1, i);
            prnComplexStmt.setInt(2, i % 500);
            prnComplexStmt.setInt(3, i % 100);
            prnComplexStmt.setString(4, "name" + i);

            prnComplexStmt.addBatch();
        }

        prnComplexStmt.executeBatch();

        U.closeQuiet(prnComplexStmt);

        conn.commit();

        U.closeQuiet(conn);

        final Collection<OrganizationKey> orgKeys = new ConcurrentLinkedQueue<>();
        final Collection<PersonKey> prnKeys = new ConcurrentLinkedQueue<>();
        final Collection<PersonComplexKey> prnComplexKeys = new ConcurrentLinkedQueue<>();

        IgniteBiInClosure<Object, Object> c = new CI2<Object, Object>() {
            @Override public void apply(Object k, Object v) {
                if (k instanceof OrganizationKey && v instanceof Organization)
                    orgKeys.add((OrganizationKey)k);
                else if (k instanceof PersonKey && v instanceof Person)
                    prnKeys.add((PersonKey)k);
                else if (k instanceof PersonComplexKey && v instanceof Person) {
                    PersonComplexKey key = (PersonComplexKey)k;

                    Person val = (Person)v;

                    assert key.getId() == val.getId();
                    assert key.getOrgId() == val.getOrgId();
                    assert ("name"  + key.getId()).equals(val.getName());

                    prnComplexKeys.add((PersonComplexKey)k);
                }
            }
        };

        store.loadCache(c);

        assertEquals(ORGANIZATION_CNT, orgKeys.size());
        assertEquals(PERSON_CNT, prnKeys.size());
        assertEquals(PERSON_CNT, prnComplexKeys.size());

        Collection<OrganizationKey> tmpOrgKeys = new ArrayList<>(orgKeys);
        Collection<PersonKey> tmpPrnKeys = new ArrayList<>(prnKeys);
        Collection<PersonComplexKey> tmpPrnComplexKeys = new ArrayList<>(prnComplexKeys);

        orgKeys.clear();
        prnKeys.clear();
        prnComplexKeys.clear();

        store.loadCache(c, OrganizationKey.class.getName(), "SELECT name, city, id FROM ORGANIZATION",
            PersonKey.class.getName(), "SELECT org_id, id, name FROM Person WHERE id < 1000");

        assertEquals(ORGANIZATION_CNT, orgKeys.size());
        assertEquals(1000, prnKeys.size());
        assertEquals(0, prnComplexKeys.size());

        store.deleteAll(tmpOrgKeys);
        store.deleteAll(tmpPrnKeys);
        store.deleteAll(tmpPrnComplexKeys);

        orgKeys.clear();
        prnKeys.clear();
        prnComplexKeys.clear();

        store.loadCache(c);

        assertTrue(orgKeys.isEmpty());
        assertTrue(prnKeys.isEmpty());
        assertTrue(prnComplexKeys.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteRetry() throws Exception {
        // Special dialect that will skip updates, to test write retry.
        BasicJdbcDialect dialect = new BasicJdbcDialect() {
            /** {@inheritDoc} */
            @Override public String updateQuery(String tblName, Collection<String> keyCols, Iterable<String> valCols) {
                return super.updateQuery(tblName, keyCols, valCols) + " AND 1 = 0";
            }
        };

        store.setDialect(dialect);

        Map<String, Map<Object, CacheAbstractJdbcStore.EntryMapping>> cacheMappings =
            GridTestUtils.getFieldValue(store, CacheAbstractJdbcStore.class, "cacheMappings");

        CacheAbstractJdbcStore.EntryMapping em = cacheMappings.get(null).get(OrganizationKey.class);

        CacheTypeMetadata typeMeta = GridTestUtils.getFieldValue(em, CacheAbstractJdbcStore.EntryMapping.class, "typeMeta");

        cacheMappings.get(null).put(OrganizationKey.class,
            new CacheAbstractJdbcStore.EntryMapping(null, dialect, typeMeta));

        Connection conn = store.openConnection(false);

        PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO Organization(id, name, city) VALUES (?, ?, ?)");

        orgStmt.setInt(1, 1);
        orgStmt.setString(2, "name" + 1);
        orgStmt.setString(3, "city" + 1);

        orgStmt.executeUpdate();

        U.closeQuiet(orgStmt);

        conn.commit();

        OrganizationKey k1 = new OrganizationKey(1);
        Organization v1 = new Organization(1, "Name1", "City1");

        ses.newSession(null);

        try {
            store.write(new CacheEntryImpl<>(k1, v1));
        }
        catch (CacheWriterException e) {
            if (!e.getMessage().startsWith("Failed insert entry in database, violate a unique index or primary key") ||
                e.getSuppressed().length != 2)
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimestamp() throws Exception {
        Timestamp k = new Timestamp(System.currentTimeMillis());

        ses.newSession(null);

        Integer v = 5;

        store.write(new CacheEntryImpl<>(k, v));

        assertEquals(v, store.load(k));

        store.delete(k);

        assertNull(store.load(k));
    }
}
