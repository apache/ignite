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

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for result set metadata.
 */
public class SqlResultSetMetaSelfTest extends AbstractIndexingCommonTest {
    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        node = null;
    }

    /**
     * Test that scale and precision returned correctly for Decimal column in result set:
     *
     * 1. Start node;
     * 2. Create table with Decimal(3,0) column;
     * 3. Insert a new row into the table;
     * 4. Execute select with decimal row;
     * 5. Check that selected decimal column has precision 3 and scale 0.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalColumnScaleAndPrecision() throws Exception {
        GridQueryProcessor qryProc = node.context().query();

        qryProc.querySqlFields(
                new SqlFieldsQuery("CREATE TABLE Person(id int, age decimal(3,0), primary key (id))")
                        .setSchema("PUBLIC"), true);

        qryProc.querySqlFields(
                new SqlFieldsQuery("INSERT INTO Person(id, age) VALUES(?, ?)")
                        .setSchema("PUBLIC")
                        .setArgs(1, new BigDecimal(160)), true);

        QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>)qryProc.querySqlFields(
                new SqlFieldsQuery("SELECT age FROM Person")
                        .setSchema("PUBLIC"), true);

        List<GridQueryFieldMetadata> fieldsMeta = cursor.fieldsMeta();

        assertEquals(1, fieldsMeta.size());

        GridQueryFieldMetadata meta = fieldsMeta.get(0);

        assertEquals(3, meta.precision());
        assertEquals(0, meta.scale());
    }
}
