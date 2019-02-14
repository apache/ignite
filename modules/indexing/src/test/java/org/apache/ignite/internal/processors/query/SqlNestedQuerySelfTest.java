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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for schemas.
 */
public class SqlNestedQuerySelfTest extends AbstractIndexingCommonTest {
    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     */
    @Test
    public void testNestingQuery() {
        sql("CREATE TABLE txs(txId INTEGER PRIMARY KEY, created INTEGER)");
        sql("CREATE TABLE ops(id INTEGER PRIMARY KEY, txId INTEGER, stage VARCHAR, tStamp INTEGER)");

        sql("INSERT INTO txs(txId, created) VALUES (1, 599000), (2, 599111), (3, 599234)");
        sql("INSERT INTO ops(id, txId, stage, tStamp) VALUES" +
            " (1, 1, 'NEW', 599686), (2, 1, 'OLD', 599722), (3, 1, 'OLD', 599736), (4, 2, 'NEW', 599767)");

        sql("WITH cacheJoin (txId, stage, tStamp)" +
              " AS (SELECT t.txId, o.stage, o.tStamp FROM txs t INNER JOIN ops o ON t.txId = o.txId)" +
            " SELECT ou.stage, COUNT(*) as cou, SUM(CASE WHEN ou.stage = in.stage THEN 1 ELSE 0 END) AS ttl" +
              " FROM (SELECT txId, stage FROM cacheJoin cte GROUP BY txId, stage) ou" +
                " INNER JOIN (SELECT mx.txId, mx.stage FROM (SELECT txId, tStamp, stage FROM cacheJoin cte) mx" +
                  " INNER JOIN (SELECT txId, MAX(tStamp) AS maxTStamp FROM cacheJoin cte GROUP BY txId) mix" +
                    " ON mx.txId = mix.txId AND mx.tStamp = mix.maxTStamp) in ON ou.txId = in.txId" +
            " GROUP BY ou.stage");
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private List<List<?>> sql(String sql) {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
