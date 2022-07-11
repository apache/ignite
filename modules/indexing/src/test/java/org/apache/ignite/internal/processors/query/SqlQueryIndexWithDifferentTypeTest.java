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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests query indexed field by value of another type. */
public class SqlQueryIndexWithDifferentTypeTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx node;

    /** */
    @Test
    public void testQueryIndex() throws Exception {
        node = startGrid();

        query("CREATE TABLE tbl(id INT PRIMARY KEY, longCol BIGINT, strCol VARCHAR, tsCol TIMESTAMP)");
        query("CREATE INDEX ON tbl(longCol)");
        query("CREATE INDEX ON tbl(strCol)");
        query("CREATE INDEX ON tbl(tsCol)");

        Calendar cal = Calendar.getInstance(TimeZone.getDefault());

        cal.clear();
        cal.set(2000, Calendar.JANUARY, 1);

        for (int i = 0; i < 100; i++) {
            query("INSERT INTO tbl VALUES(?, ?, ?, ?)",
                i, (long)i, Integer.valueOf(i).toString(), new Timestamp(cal.getTimeInMillis()));

            cal.add(Calendar.DAY_OF_MONTH, 1);
        }

        // Find "int" value in "long" index key.
        assertEquals(50L, query("SELECT count(*) FROM tbl WHERE longCol >= ?", 50).get(0).get(0));
        // Find "string" value in "long" index key (dynamic parameter value converted to indexed type).
        assertEquals(50L, query("SELECT count(*) FROM tbl WHERE longCol >= ?", "50").get(0).get(0));
        // Find "int" value in "string" index key (indexed value converted to dynamic parameter type).
        assertEquals(50L, query("SELECT count(*) FROM tbl WHERE strCol >= ?", 50).get(0).get(0));
        // Find "string" value in "long" index key (literal value converted to indexed type).
        assertEquals(50L, query("SELECT count(*) FROM tbl WHERE longCol >= '50'").get(0).get(0));
        // Find "string" value in "timestamp" index key (literal value converted to indexed type).
        assertEquals(50L, query("SELECT count(*) FROM tbl WHERE tsCol >= '2000-02-20'").get(0).get(0));

        query("CREATE TABLE tbl2(id INT PRIMARY KEY, longCol BIGINT, strNum VARCHAR, strDate VARCHAR)");
        query("INSERT INTO tbl2 VALUES(?, ?, ?, ?)", 50, 50L, "50", "2000-02-20");

        // Check find by cache row.
        assertEquals(50, query("SELECT tbl.id FROM tbl JOIN tbl2 ON tbl.longCol = strNum").get(0).get(0));
        assertEquals(50, query("SELECT tbl.id FROM tbl JOIN tbl2 ON tbl.tsCol = strDate").get(0).get(0));
        assertEquals(50, query("SELECT tbl.id FROM tbl JOIN tbl2 ON tbl.strCol = tbl2.longCol").get(0).get(0));
    }

    /** */
    private List<List<?>> query(String qry, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false).getAll();
    }
}
