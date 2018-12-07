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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for statement reuse.
 */
public class SqlLocalQueryConnectionAndStatementTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    public void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     */
    public void testReplicated() {
        sql("CREATE TABLE repl_tbl (id LONG PRIMARY KEY, val LONG) WITH \"template=replicated\"").getAll();

        for (int i = 0; i < 10; i++)
            sql("insert into repl_tbl(id,val) VALUES(" + i + "," + i + ")").getAll();

        Iterator<List<?>> it0 = sql(new SqlFieldsQuery("SELECT * FROM tbl where id > ?").setArgs(1)).iterator();

        it0.next();

        sql(new SqlFieldsQuery("SELECT * FROM tbl where id > ?").setArgs(1)).getAll();

        it0.next();
    }

    /**
     */
    public void testLocalQuery() {
        sql("CREATE TABLE tbl (id LONG PRIMARY KEY, val LONG)").getAll();

        for (int i = 0; i < 10; i++)
            sql("insert into tbl(id,val) VALUES(" + i + "," + i + ")").getAll();

        Iterator<List<?>> it0 = sql(new SqlFieldsQuery("SELECT * FROM tbl where id > ?").setArgs(1).setLocal(true)).iterator();

        it0.next();

        sql(new SqlFieldsQuery("SELECT * FROM tbl where id > ?").setArgs(1).setLocal(true)).getAll();

        it0.next();
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private FieldsQueryCursor<List<?>> sql(String sql) {
        return sql(new SqlFieldsQuery(sql));
    }

    /**
     * @param qry SQL query.
     * @return Results.
     */
    private FieldsQueryCursor<List<?>> sql(SqlFieldsQuery qry) {
        GridQueryProcessor qryProc = grid(0).context().query();

        return qryProc.querySqlFields(qry, true);
    }
}
