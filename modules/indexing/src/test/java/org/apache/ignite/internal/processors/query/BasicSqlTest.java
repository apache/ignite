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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Basic simple tests for SQL.
 */
public class BasicSqlTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    /**
     */
    @Test
    public void validateKeyType() throws Exception {
        sql("CREATE TABLE TEST (ID0 INT, ID1 INT, VAL0 INT, VAL1 INT, PRIMARY KEY(ID0, ID1)) "
                + "WITH \"CACHE_NAME=test,KEY_TYPE=TestType,VALUE_TYPE=VAL_TYPE\"");

        BinaryObjectBuilder bobKey0 = grid(0).binary().builder("key0");
        bobKey0.setField("ID0", 0);
        bobKey0.setField("ID1", 0);

        final BinaryObject key0 = bobKey0.build();

        BinaryObjectBuilder bobVal0 = grid(0).binary().builder("VAL_TYPE");
        bobVal0.setField("VAL0", 0);
        bobVal0.setField("VAL1", 0);

        // Put key with invalid type.
        GridTestUtils.assertThrowsAnyCause(
                log,
                () -> {
                    grid(0).cache("test").put(key0, bobVal0.build());
                    return null;
                },
                IgniteSQLException.class,
                "Key type not is allowed for table [table=TEST, expectedKeyType=TestType, actualType=key0]"
        );

        bobKey0 = grid(0).binary().builder("TestType");
        bobKey0.setField("ID0", 0);
        bobKey0.setField("ID1", 0);

        // Put proper key.
        grid(0).cache("test").put(bobKey0.build(), bobVal0.build());
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
