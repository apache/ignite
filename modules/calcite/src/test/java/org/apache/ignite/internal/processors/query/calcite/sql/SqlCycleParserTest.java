/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests CYCLE syntax, AST copying and unparsing independently of validation. */
public class SqlCycleParserTest {
    /** Complete and abbreviated clauses retain all five operands. */
    @ParameterizedTest
    @ValueSource(strings = {"CYCLE n SET c USING p", "CYCLE n, k SET c USING p",
        "CYCLE n, k SET c TO 'Y' DEFAULT 'N' USING p"})
    public void testRoundTrip(String clause) throws Exception {
        SqlWith with = parse("WITH RECURSIVE t(n, k) AS (VALUES (1, 2) UNION ALL SELECT * FROM t)\n" +
            clause + " SELECT * FROM t");
        IgniteSqlWithItem item = (IgniteSqlWithItem)with.withList.get(0);

        assertEquals(5, item.getOperandList().size());
        assertEquals(5, item.cycleClause.getOperandList().size());
        assertEquals(2, item.cycleClause.getParserPosition().getLineNum());
        assertEquals("C", item.cycleClause.markColumn.getSimple());
        assertEquals("P", item.cycleClause.pathColumn.getSimple());

        String sql = with.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
        SqlWith reparsed = parse(sql);

        assertEquals(sql, reparsed.toSqlString(CalciteSqlDialect.DEFAULT).getSql());

        SqlNode copied = with.accept(new SqlShuttle() {
            @Override public SqlNode visit(SqlLiteral literal) {
                return SqlNode.clone(literal);
            }
        });

        assertEquals(sql, copied.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
    }

    /** The abbreviated syntax materializes TRUE and FALSE in the AST. */
    @Test
    public void testDefaultMarks() throws Exception {
        SqlWith with = parse("WITH RECURSIVE t(n) AS (VALUES (1)) CYCLE n SET c USING p SELECT * FROM t");
        SqlCycleClause cycle = ((IgniteSqlWithItem)with.withList.get(0)).cycleClause;

        assertTrue(((SqlLiteral)cycle.markValue).booleanValue());
        assertFalse(((SqlLiteral)cycle.defaultValue).booleanValue());
    }

    /** Missing required pieces of CYCLE are syntax errors. */
    @ParameterizedTest
    @ValueSource(strings = {"CYCLE SET c USING p", "CYCLE n USING p", "CYCLE n SET c",
        "CYCLE n SET c TO 'Y' USING p", "CYCLE n SET c DEFAULT 'N' USING p",
        "CYCLE n, SET c USING p"})
    public void testIncompleteClause(String clause) {
        assertThrows(SqlParseException.class,
            () -> parse("WITH RECURSIVE t(n) AS (VALUES (1)) " + clause + " SELECT * FROM t"));
    }

    /** Parses with Ignite's generated grammar. */
    private static SqlWith parse(String sql) throws SqlParseException {
        return (SqlWith)SqlParser.create(sql,
            SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY)).parseQuery();
    }
}
