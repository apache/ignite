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

package org.apache.ignite.internal.processors.cache;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests parsing of {@code SELECT ... FOR UPDATE} in Calcite engine. */
public class SelectForUpdateParsingTest {
    /**
     * Ensures that adding {@code FOR UPDATE} does not change the query text representation
     * while parser still keeps explicit flag in {@link SqlSelect}.
     */
    @Test
    public void testSelectForUpdateClauseIsIgnored() throws SqlParseException {
        SqlOrderBy withoutForUpdate = (SqlOrderBy)parse("SELECT id, val FROM TEST ORDER BY id");
        SqlOrderBy withForUpdate = (SqlOrderBy)parse("SELECT id, val FROM TEST ORDER BY id FOR UPDATE");

        SqlSelect selectWithoutForUpdate = (SqlSelect)withoutForUpdate.query;
        SqlSelect selectWithForUpdate = (SqlSelect)withForUpdate.query;

        assertEquals(withoutForUpdate.toString(), withForUpdate.toString());
        assertFalse(selectWithoutForUpdate.isForUpdate());
        assertTrue(selectWithForUpdate.isForUpdate());
    }

    /**
     * Parses SQL statement with Ignite Calcite parser.
     *
     * @param sql SQL text.
     * @return Parsed SQL node.
     * @throws SqlParseException If parsing fails.
     */
    private static SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(
            sql,
            SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY)
        );

        return parser.parseStmt();
    }
}
