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

package org.apache.ignite.internal.processors.query.calcite.sql;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for parsing {@code SELECT ... FOR UPDATE} syntax. */
public class SqlSelectForUpdateParserTest extends GridCommonAbstractTest {
    /** Regular SELECT is returned as-is (no wrapping). */
    @Test
    public void regularSelectIsNotWrapped() throws SqlParseException {
        SqlNode node = parse("SELECT name FROM Person");

        assertThat(node, instanceOf(SqlSelect.class));
    }

    /** Minimal FOR UPDATE with no options. */
    @Test
    public void forUpdateNoOptions() throws SqlParseException {
        SqlNode node = parse("SELECT name FROM Person FOR UPDATE");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        assertThat(forUpdate.query(), instanceOf(SqlSelect.class));
        assertThat(forUpdate.ofList(), nullValue());
        assertThat(forUpdate.waitSeconds(), nullValue());
    }

    /** FOR UPDATE with a single OF column (unqualified). */
    @Test
    public void forUpdateOfSingleColumn() throws SqlParseException {
        SqlNode node = parse("SELECT id, name FROM Person FOR UPDATE OF name");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        assertThat(forUpdate.ofList().size(), is(1));
        assertThat(((SqlIdentifier)forUpdate.ofList().get(0)).getSimple(), is("NAME"));
        assertThat(forUpdate.waitSeconds(), nullValue());
    }

    /** FOR UPDATE with a qualified OF column (table.column). */
    @Test
    public void forUpdateOfQualifiedColumn() throws SqlParseException {
        SqlNode node = parse("SELECT p.id FROM Person p FOR UPDATE OF p.id");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        SqlIdentifier col = (SqlIdentifier)forUpdate.ofList().get(0);

        assertThat(col.names.get(0), is("P"));
        assertThat(col.names.get(1), is("ID"));
    }

    /** FOR UPDATE with multiple OF columns from different tables. */
    @Test
    public void forUpdateOfMultipleColumns() throws SqlParseException {
        SqlNode node = parse(
            "SELECT p.id, o.total FROM Person p JOIN Orders o ON p.id = o.pid " +
            "FOR UPDATE OF p.id, o.total");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        assertThat(forUpdate.ofList().size(), is(2));
    }

    /** FOR UPDATE WAIT n stores the timeout in seconds. */
    @Test
    public void forUpdateWait() throws SqlParseException {
        SqlNode node = parse("SELECT name FROM Person FOR UPDATE WAIT 10");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        assertThat(forUpdate.waitSeconds(), is(10L));
    }

    /** FOR UPDATE NOWAIT stores waitSeconds = 0. */
    @Test
    public void forUpdateNowait() throws SqlParseException {
        SqlNode node = parse("SELECT name FROM Person FOR UPDATE NOWAIT");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        assertThat(forUpdate.waitSeconds(), is(0L));
    }

    /** FOR UPDATE with both OF and WAIT. */
    @Test
    public void forUpdateOfAndWait() throws SqlParseException {
        SqlNode node = parse("SELECT id, name FROM Person FOR UPDATE OF name WAIT 5");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        IgniteSqlSelectForUpdate forUpdate = (IgniteSqlSelectForUpdate)node;

        assertThat(forUpdate.ofList().size(), is(1));
        assertThat(forUpdate.waitSeconds(), is(5L));
    }

    /** FOR UPDATE with WAIT 0 (equivalent to NOWAIT). */
    @Test
    public void forUpdateWaitZero() throws SqlParseException {
        SqlNode node = parse("SELECT name FROM Person FOR UPDATE WAIT 0");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));

        assertThat(((IgniteSqlSelectForUpdate)node).waitSeconds(), is(0L));
    }

    /** FOR UPDATE on a query with ORDER BY and LIMIT. */
    @Test
    public void forUpdateWithOrderByAndLimit() throws SqlParseException {
        SqlNode node = parse("SELECT name FROM Person ORDER BY id LIMIT 10 FOR UPDATE");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));
    }

    /** Regular WITH query is returned as-is (no FOR UPDATE wrapping). */
    @Test
    public void withQueryIsNotWrapped() throws SqlParseException {
        SqlNode node = parse("WITH cte AS (SELECT id FROM Person) SELECT id FROM cte");

        assertThat(node, instanceOf(SqlNode.class));
        // Must NOT be wrapped in IgniteSqlSelectForUpdate
        assertThat(node instanceof IgniteSqlSelectForUpdate, is(false));
    }

    /** WITH query followed by FOR UPDATE is wrapped. */
    @Test
    public void withQueryForUpdate() throws SqlParseException {
        SqlNode node = parse("WITH cte AS (SELECT id FROM Person) SELECT id FROM cte FOR UPDATE");

        assertThat(node, instanceOf(IgniteSqlSelectForUpdate.class));
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Parses a single SQL statement via {@code parseStmtList()}, which goes through
     * {@code SqlStmt()} and therefore through our {@code statementParserMethods},
     * including {@code SqlSelectForUpdate()}.
     */
    private static SqlNode parse(String sql) throws SqlParseException {
        SqlNodeList stmts = SqlParser.create(sql,
            SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY))
            .parseStmtList();

        assertEquals(1, stmts.size());

        return stmts.get(0);
    }
}
