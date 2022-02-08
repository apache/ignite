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

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
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
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }

    /**
     * @param qry Query.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> execute(SqlFieldsQuery qry) {
        return grid(0).context().query().querySqlFields(qry, false);
    }
}
