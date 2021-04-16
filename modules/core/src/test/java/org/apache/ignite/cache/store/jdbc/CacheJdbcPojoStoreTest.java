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

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.model.BinaryTest;
import org.apache.ignite.cache.store.jdbc.model.BinaryTestKey;
import org.apache.ignite.cache.store.jdbc.model.Organization;
import org.apache.ignite.cache.store.jdbc.model.OrganizationKey;
import org.apache.ignite.cache.store.jdbc.model.Person;
import org.apache.ignite.cache.store.jdbc.model.PersonComplexKey;
import org.apache.ignite.cache.store.jdbc.model.PersonKey;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.cache.GridAbstractCacheStoreSelfTest;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Test;

/**
 * Class for {@code PojoCacheStore} tests.
 */
public class CacheJdbcPojoStoreTest extends GridAbstractCacheStoreSelfTest<CacheJdbcPojoStore<Object, Object>> {
    /** DB connection URL. */
    private static final String DFLT_CONN_URL = "jdbc:h2:mem:autoCacheStore;DB_CLOSE_DELAY=-1";

    /** Organization count. */
    protected static final int ORGANIZATION_CNT = 1000;

    /** Person count. */
    protected static final int PERSON_CNT = 100000;

    /** Ignite. */
    private Ignite ig;

    /** Binary enable. */
    private boolean binaryEnable;

    /**
     * @throws Exception If failed.
     */
    public CacheJdbcPojoStoreTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected CacheJdbcPojoStore<Object, Object> store() {
        CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();

        JdbcType[] storeTypes = new JdbcType[7];

        storeTypes[0] = new JdbcType();
        storeTypes[0].setDatabaseSchema("PUBLIC");
        storeTypes[0].setDatabaseTable("ORGANIZATION");
        storeTypes[0].setKeyType("org.apache.ignite.cache.store.jdbc.model.OrganizationKey");
        storeTypes[0].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));

        storeTypes[0].setValueType("org.apache.ignite.cache.store.jdbc.model.Organization");
        storeTypes[0].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"),
            new JdbcTypeField(Types.VARCHAR, "CITY", String.class, "city"));

        storeTypes[1] = new JdbcType();
        storeTypes[1].setDatabaseSchema("PUBLIC");
        storeTypes[1].setDatabaseTable("PERSON");
        storeTypes[1].setKeyType("org.apache.ignite.cache.store.jdbc.model.PersonKey");
        storeTypes[1].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));

        storeTypes[1].setValueType("org.apache.ignite.cache.store.jdbc.model.Person");
        storeTypes[1].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", Integer.class, "orgId"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"));

        storeTypes[2] = new JdbcType();
        storeTypes[2].setDatabaseSchema("PUBLIC");
        storeTypes[2].setDatabaseTable("PERSON_COMPLEX");
        storeTypes[2].setKeyType("org.apache.ignite.cache.store.jdbc.model.PersonComplexKey");
        storeTypes[2].setKeyFields(
            new JdbcTypeField(Types.INTEGER, "ID", int.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", int.class, "orgId"),
            new JdbcTypeField(Types.INTEGER, "CITY_ID", int.class, "cityId"));

        storeTypes[2].setValueType("org.apache.ignite.cache.store.jdbc.model.Person");
        storeTypes[2].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", Integer.class, "orgId"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"),
            new JdbcTypeField(Types.INTEGER, "SALARY", Integer.class, "salary"));

        storeTypes[3] = new JdbcType();
        storeTypes[3].setDatabaseSchema("PUBLIC");
        storeTypes[3].setDatabaseTable("TIMESTAMP_ENTRIES");
        storeTypes[3].setKeyType("java.sql.Timestamp");
        storeTypes[3].setKeyFields(new JdbcTypeField(Types.TIMESTAMP, "KEY", Timestamp.class, null));

        storeTypes[3].setValueType("java.lang.Integer");
        storeTypes[3].setValueFields(new JdbcTypeField(Types.INTEGER, "VAL", Integer.class, null));

        storeTypes[4] = new JdbcType();
        storeTypes[4].setDatabaseSchema("PUBLIC");
        storeTypes[4].setDatabaseTable("STRING_ENTRIES");
        storeTypes[4].setKeyType("java.lang.String");
        storeTypes[4].setKeyFields(new JdbcTypeField(Types.VARCHAR, "KEY", String.class, null));

        storeTypes[4].setValueType("java.lang.String");
        storeTypes[4].setValueFields(new JdbcTypeField(Types.VARCHAR, "VAL", Integer.class, null));

        storeTypes[5] = new JdbcType();
        storeTypes[5].setDatabaseSchema("PUBLIC");
        storeTypes[5].setDatabaseTable("UUID_ENTRIES");
        storeTypes[5].setKeyType("java.util.UUID");
        storeTypes[5].setKeyFields(new JdbcTypeField(Types.BINARY, "KEY", UUID.class, null));

        storeTypes[5].setValueType("java.util.UUID");
        storeTypes[5].setValueFields(new JdbcTypeField(Types.BINARY, "VAL", UUID.class, null));

        storeTypes[6] = new JdbcType();
        storeTypes[6].setDatabaseSchema("PUBLIC");
        storeTypes[6].setDatabaseTable("BINARY_ENTRIES");
        storeTypes[6].setKeyType("org.apache.ignite.cache.store.jdbc.model.BinaryTestKey");
        storeTypes[6].setKeyFields(new JdbcTypeField(Types.BINARY, "KEY", Integer.class, "id"));

        storeTypes[6].setValueType("org.apache.ignite.cache.store.jdbc.model.BinaryTest");
        storeTypes[6].setValueFields(new JdbcTypeField(Types.BINARY, "VAL", byte[].class, "bytes"));

        storeFactory.setTypes(storeTypes);

        storeFactory.setDialect(new H2Dialect());

        CacheJdbcPojoStore<Object, Object> store = storeFactory.create();

        // H2 DataSource
        store.setDataSource(JdbcConnectionPool.create(DFLT_CONN_URL, "sa", ""));

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

        try {
            stmt.executeUpdate("delete from Binary_Entries");
        }
        catch (SQLException ignore) {
            // No-op.
        }

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "String_Entries (key varchar(100) not null, val varchar(100), PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "UUID_Entries (key binary(16) not null, val binary(16), PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Binary_Entries (key binary(16) not null, val binary(16), PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Timestamp_Entries (key timestamp not null, val integer, PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Organization (id integer not null, name varchar(50), city varchar(50), PRIMARY KEY(id))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Person (id integer not null, org_id integer, name varchar(50), PRIMARY KEY(id))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Person_Complex (id integer not null, org_id integer not null, city_id integer not null, " +
            "name varchar(50), salary integer, PRIMARY KEY(id, org_id, city_id))");

        conn.commit();

        U.closeQuiet(stmt);

        U.closeQuiet(conn);

        super.beforeTest();

        Ignite ig = U.field(store, "ignite");

        this.ig = ig;

        binaryEnable = ig.configuration().getMarshaller() instanceof BinaryMarshaller;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

        PreparedStatement prnComplexStmt = conn.prepareStatement("INSERT INTO Person_Complex(id, org_id, city_id, name, salary) VALUES (?, ?, ?, ?, ?)");

        for (int i = 0; i < PERSON_CNT; i++) {
            prnComplexStmt.setInt(1, i);
            prnComplexStmt.setInt(2, i % 500);
            prnComplexStmt.setInt(3, i % 100);
            prnComplexStmt.setString(4, "name" + i);

            if (i > 0)
                prnComplexStmt.setInt(5, 1000 + i * 500);
            else // Add person with null salary
                prnComplexStmt.setNull(5, Types.INTEGER);

            prnComplexStmt.addBatch();
        }

        prnComplexStmt.executeBatch();

        U.closeQuiet(prnComplexStmt);

        conn.commit();

        U.closeQuiet(prnStmt);

        PreparedStatement binaryStmt = conn.prepareStatement("INSERT INTO Binary_Entries(key, val) VALUES (?, ?)");

        byte[] bytes = new byte[16];

        for (byte i = 0; i < 16; i++)
            bytes[i] = i;

        binaryStmt.setInt(1, 1);
        binaryStmt.setBinaryStream(2, new ByteArrayInputStream(bytes));

        binaryStmt.addBatch();
        binaryStmt.executeBatch();

        U.closeQuiet(binaryStmt);

        conn.commit();

        U.closeQuiet(conn);

        final Collection<Object> orgKeys = new ConcurrentLinkedQueue<>();
        final Collection<Object> prnKeys = new ConcurrentLinkedQueue<>();
        final Collection<Object> prnComplexKeys = new ConcurrentLinkedQueue<>();
        final Collection<Object> binaryTestVals = new ConcurrentLinkedQueue<>();

        IgniteBiInClosure<Object, Object> c = new CI2<Object, Object>() {
            @Override public void apply(Object k, Object v) {
                if (binaryEnable) {
                    if (k instanceof BinaryObject && v instanceof BinaryObject) {
                        BinaryObject key = (BinaryObject)k;
                        BinaryObject val = (BinaryObject)v;

                        String keyType = key.type().typeName();
                        String valType = val.type().typeName();

                        if (OrganizationKey.class.getName().equals(keyType)
                            && Organization.class.getName().equals(valType))
                            orgKeys.add(key);

                        if (PersonKey.class.getName().equals(keyType)
                            && Person.class.getName().equals(valType))
                            prnKeys.add(key);

                        if (PersonComplexKey.class.getName().equals(keyType)
                            && Person.class.getName().equals(valType))
                            prnComplexKeys.add(key);

                        if (BinaryTestKey.class.getName().equals(keyType)
                            && BinaryTest.class.getName().equals(valType))
                            binaryTestVals.add(val.field("bytes"));
                    }
                } else {
                    if (k instanceof OrganizationKey && v instanceof Organization)
                        orgKeys.add(k);
                    else if (k instanceof PersonKey && v instanceof Person)
                        prnKeys.add(k);
                    else if (k instanceof BinaryTestKey && v instanceof BinaryTest)
                        binaryTestVals.add(((BinaryTest)v).getBytes());
                    else if (k instanceof PersonComplexKey && v instanceof Person) {
                        PersonComplexKey key = (PersonComplexKey)k;

                        Person val = (Person)v;

                        assertTrue("Key ID should be the same as value ID", key.getId() == val.getId());
                        assertTrue("Key orgID should be the same as value orgID", key.getOrgId() == val.getOrgId());
                        assertEquals("name" + key.getId(), val.getName());

                        prnComplexKeys.add(k);
                    }
                }
            }
        };

        store.loadCache(c);

        assertEquals(ORGANIZATION_CNT, orgKeys.size());
        assertEquals(PERSON_CNT, prnKeys.size());
        assertEquals(PERSON_CNT, prnComplexKeys.size());
        assertEquals(1, binaryTestVals.size());
        assertTrue(Arrays.equals(bytes, (byte[])binaryTestVals.iterator().next()));

        Collection<Object> tmpOrgKeys = new ArrayList<>(orgKeys);
        Collection<Object> tmpPrnKeys = new ArrayList<>(prnKeys);
        Collection<Object> tmpPrnComplexKeys = new ArrayList<>(prnComplexKeys);

        orgKeys.clear();
        prnKeys.clear();
        prnComplexKeys.clear();

        store.loadCache(
            c, OrganizationKey.class.getName(), "SELECT name, city, id FROM ORGANIZATION",
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
    @Test
    public void testParallelLoad() throws Exception {
        Connection conn = store.openConnection(false);

        PreparedStatement prnComplexStmt = conn.prepareStatement("INSERT INTO Person_Complex(id, org_id, city_id, name, salary) VALUES (?, ?, ?, ?, ?)");

        for (int i = 0; i < 8; i++) {

            prnComplexStmt.setInt(1, (i >> 2) & 1);
            prnComplexStmt.setInt(2, (i >> 1) & 1);
            prnComplexStmt.setInt(3, i % 2);

            prnComplexStmt.setString(4, "name");
            prnComplexStmt.setInt(5, 1000 + i * 500);

            prnComplexStmt.addBatch();
        }

        prnComplexStmt.executeBatch();

        U.closeQuiet(prnComplexStmt);

        conn.commit();

        U.closeQuiet(conn);

        final Collection<Object> prnComplexKeys = new ConcurrentLinkedQueue<>();

        IgniteBiInClosure<Object, Object> c = new CI2<Object, Object>() {
            @Override public void apply(Object k, Object v) {
                if (binaryEnable) {
                    if (k instanceof BinaryObject && v instanceof BinaryObject) {
                        BinaryObject key = (BinaryObject)k;
                        BinaryObject val = (BinaryObject)v;

                        String keyType = key.type().typeName();
                        String valType = val.type().typeName();

                        if (PersonComplexKey.class.getName().equals(keyType)
                            && Person.class.getName().equals(valType))
                            prnComplexKeys.add(key);
                    }
                }
                else {
                    if (k instanceof PersonComplexKey && v instanceof Person)
                        prnComplexKeys.add(k);
                    else
                        fail("Unexpected entry [key=" + k + ", value=" + v + "]");
                }
            }
        };

        store.setParallelLoadCacheMinimumThreshold(2);

        store.loadCache(c);

        assertEquals(8, prnComplexKeys.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWriteRetry() throws Exception {
        CacheJdbcPojoStore<Object, Object> store = store();

        // Special dialect that will skip updates, to test write retry.
        store.setDialect(new H2Dialect() {
            /** {@inheritDoc} */
            @Override public boolean hasMerge() {
                return false;
            }

            /** {@inheritDoc} */
            @Override public String updateQuery(String tblName, Collection<String> keyCols,
                Iterable<String> valCols) {
                return super.updateQuery(tblName, keyCols, valCols) + " AND 1 = 0";
            }
        });

        inject(store);

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
            store.write(new CacheEntryImpl<>(wrap(k1), wrap(v1)));

            fail("CacheWriterException wasn't thrown.");
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
    @Test
    public void testTimestamp() throws Exception {
        Timestamp k = new Timestamp(System.currentTimeMillis());

        ses.newSession(null);

        Integer v = 5;

        store.write(new CacheEntryImpl<>(k, v));

        assertEquals(v, store.load(k));

        store.delete(k);

        assertNull(store.load(k));
    }

    /**
     * @param obj Object.
     */
    private Object wrap(Object obj) throws IllegalAccessException {
        if (binaryEnable) {
            Class<?> cls = obj.getClass();

            BinaryObjectBuilder builder = ig.binary().builder(cls.getName());

            for (Field f : cls.getDeclaredFields()) {
                if (f.getName().contains("serialVersionUID"))
                    continue;

                f.setAccessible(true);

                builder.setField(f.getName(), f.get(obj));
            }

            return builder.build();
        }

        return obj;
    }
}
