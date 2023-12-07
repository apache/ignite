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

package org.apache.ignite.internal.processors.query;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Checks the impossibility of inserting duplicate keys in case part of the key or the whole key is null or not set.
 */
public class IgniteInsertNullableDuplicatesSqlTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    /**
     * Checks the impossibility of inserting duplicate keys in case part of the key is missing or null.
     */
    @Test
    public void testInsertKeyWithNullKeyParts() {
        sql("CREATE TABLE test (id1 INT, id2 INT, val INT, CONSTRAINT PK PRIMARY KEY(id1, id2))");
        sql("insert into test (id1, id2, val) values (1, null, 1);");
        
        assertThrows(log,
            () -> sql("insert into test (id1, id2, val) values (1, null, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertThrows(log,
            () -> sql("insert into test (id1, val) values (1, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertEquals(sql("SELECT * FROM test").getAll().size(), 1);
    }

    /**
     * Checks the impossibility of inserting duplicate keys in case the whole is missing or null.
     */
    @Test
    public void testInsertKeyWithNullKeys() {
        sql("CREATE TABLE test (id1 INT, id2 INT, val INT, CONSTRAINT PK PRIMARY KEY(id1, id2))");
        sql("insert into test (id1, id2, val) values (null, null, 1);");
        
        assertThrows(log,
            () -> sql("insert into test (id1, val) values (null, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertThrows(log,
            () -> sql("insert into test (id2, val) values (null, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertThrows(log,
            () -> sql("insert into test (id2, id1, val) values (null, null, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertEquals(sql("SELECT * FROM test").getAll().size(), 1);
    }

    /**
     * Checks the impossibility of inserting duplicate keys in case key is not set.
     */
    @Test
    public void testInsertKeyWhenKeyIsNotSet() {
        sql("CREATE TABLE test (id1 INT, id2 INT, val INT, CONSTRAINT PK PRIMARY KEY(id1, id2))");
        sql("insert into test (val) values (1);");
        assertThrows(log,
            () -> sql("insert into test (val) values (1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");
    }

    /**
     * Checks the impossibility of inserting duplicate keys in case key is not set and default values is set.
     */
    @Test
    public void testInsertKeyWithNullKeyPartsDefault() {
        sql("CREATE TABLE test (id1 INT, id2 INT DEFAULT 20, val INT, CONSTRAINT PK PRIMARY KEY(id1, id2))");
    
        sql("insert into test (id1, val) values (0, 1);");
        sql("insert into test (val) values (2);");
    
        assertThrows(log,
                () -> sql("insert into test (id1, val) values (0, 1);"),
                TransactionDuplicateKeyException.class,
                "Duplicate key during INSERT");
    
        assertThrows(log,
                () -> sql("insert into test (val) values (2);"),
                TransactionDuplicateKeyException.class,
                "Duplicate key during INSERT");
    
        List<List<?>> sql = sql("select * from test order by val asc;").getAll();
    
        assertEquals(sql.size(), 2);
    
        assertEquals(sql.get(0).get(0), 0);
        assertEquals(sql.get(0).get(1), 20);
        assertEquals(sql.get(0).get(2), 1);
    
        assertNull(sql.get(1).get(0));
        assertEquals(sql.get(1).get(1), 20);
        assertEquals(sql.get(1).get(2), 2);
    }
    
    /**
     * Same test as above, but with table created by cache api.
     */
    @Test
    public void testInsertKeyWithNullKeyPartsDefaultCacheApi() {
        Set<String> keyFields = new LinkedHashSet<>();
        keyFields.add("ID1");
        keyFields.add("ID2");
        
        Map<String, Object> defsFK2 = new HashMap<>();
        defsFK2.put("ID2", 20);
        
        grid(0).getOrCreateCache(
                new CacheConfiguration<>("test")
                        .setSqlSchema("PUBLIC")
                        .setQueryEntities(F.asList(
                                new QueryEntity("MY_KEY_TYPE", "MY_VALUE_TYPE")
                                        .setTableName("TEST")
                                        .addQueryField("ID1", Integer.class.getName(), "ID1")
                                        .addQueryField("ID2", Integer.class.getName(), "ID2")
                                        .addQueryField("VAL", Integer.class.getName(), "VAL")
                                        .setKeyFields(keyFields)
                                        .setDefaultFieldValues(defsFK2)
                        ))
        );
    
        sql("insert into test (id1, val) values (0, 1);");
        sql("insert into test (val) values (2);");
    
        assertThrows(log,
                () -> sql("insert into test (id1, val) values (0, 1);"),
                TransactionDuplicateKeyException.class,
                "Duplicate key during INSERT");
        
        assertThrows(log,
                () -> sql("insert into test (val) values (2);"),
                TransactionDuplicateKeyException.class,
                "Duplicate key during INSERT");
        
        List<List<?>> sql = sql("select * from test order by val asc;").getAll();
    
        assertEquals(sql.size(), 2);
        
        assertEquals(sql.get(0).get(0), 0);
        assertEquals(sql.get(0).get(1), 20);
        assertEquals(sql.get(0).get(2), 1);
    
        assertNull(sql.get(1).get(0));
        assertEquals(sql.get(1).get(1), 20);
        assertEquals(sql.get(1).get(2), 2);
    }

    /**
     * Same test as above, but with table created by cache api.
     */
    @Test
    public void testInsertKeyWithNullKeyParts2() {
        Set<String> keyFields = new LinkedHashSet<>();
        keyFields.add("ID1");
        keyFields.add("ID2");

        grid(0).getOrCreateCache(
            new CacheConfiguration<>("test")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(F.asList(
                    new QueryEntity()
                        .setTableName("TEST")
                        .setKeyType("MY_KEY_TYPE")
                        .setValueType("MY_VALUE_TYPE")
                        .addQueryField("ID1", Integer.class.getName(), "ID1")
                        .addQueryField("ID2", Integer.class.getName(), "ID2")
                        .addQueryField("VAL", Integer.class.getName(), "VAL")
                        .setKeyFields(keyFields)
                ))
        );

        sql("insert into test (id1, id2, val) values (1, null, 1);");

        assertThrows(log,
            () -> sql("insert into test (id1, id2, val) values (1, null, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertThrows(log,
            () -> sql("insert into test (id1, val) values (1, 1);"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");

        assertEquals(sql("SELECT * FROM test").getAll().size(), 1);
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}
