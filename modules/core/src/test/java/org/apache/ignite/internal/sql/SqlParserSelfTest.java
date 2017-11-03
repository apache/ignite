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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;
import java.util.Iterator;

/**
 * Test for parser.
 */
public class SqlParserSelfTest extends GridCommonAbstractTest {
    /**
     * Tests for CREATE INDEX command.
     *
     * @throws Exception If failed.
     */
    public void testCreateIndex() throws Exception {
        SqlCreateIndexCommand cmd =
            (SqlCreateIndexCommand)new SqlParser(null, "CREATE INDEX idx ON tbl(a)").nextCommand();

        validate(cmd, null, "tbl", "idx", "a", false);
    }

    /**
     * Validate create index command.
     *
     * @param cmd Command.
     * @param expSchemaName Expected schema name.
     * @param expTblName Expected table name.
     * @param expIdxName Expected index name.
     * @param expColDefs Expected column definitions.
     */
    private static void validate(SqlCreateIndexCommand cmd, String expSchemaName, String expTblName, String expIdxName,
        Object... expColDefs) {
        assertEquals(expSchemaName, cmd.schemaName());
        assertEquals(expTblName, cmd.tableName());
        assertEquals(expIdxName, cmd.indexName());

        if (F.isEmpty(expColDefs) || expColDefs.length % 2 == 1)
            throw new IllegalArgumentException("Column definitions must be even.");

        Collection<SqlIndexColumn> cols = cmd.columns();

        assertEquals(expColDefs.length / 2, cols.size());

        Iterator<SqlIndexColumn> colIter = cols.iterator();

        for (int i = 0; i < expColDefs.length;) {
            SqlIndexColumn col = colIter.next();

            String expColName = (String)expColDefs[i++];
            Boolean expDesc = (Boolean) expColDefs[i++];

            assertEquals(expColName, col.name());
            assertEquals(expDesc, (Boolean)col.descending());
        }
    }
}
