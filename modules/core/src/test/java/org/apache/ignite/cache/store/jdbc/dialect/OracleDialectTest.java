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

package org.apache.ignite.cache.store.jdbc.dialect;

import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link OracleDialect}.
 */
public class OracleDialectTest {
    /** */
    @Test
    public void testMergeQuery() {
        OracleDialect dialect = new OracleDialect();

        assertTrue(dialect.hasMerge());

        String expQry = "MERGE INTO table t USING (SELECT ? AS col1, ? AS uniqCol FROM dual) v  ON (t.col1=v.col1) " +
            "WHEN MATCHED THEN UPDATE SET t.uniqCol = v.uniqCol " +
            "WHEN NOT MATCHED THEN  INSERT (col1, uniqCol) VALUES (v.col1, v.uniqCol)";

        assertEquals(expQry, dialect.mergeQuery("table", singleton("col1"), singleton("uniqCol")));

        expQry = "MERGE INTO table t USING (SELECT ? AS col1 FROM dual) v  ON (t.col1=v.col1) " +
            "WHEN NOT MATCHED THEN  INSERT (col1) VALUES (v.col1)";

        assertEquals(expQry, dialect.mergeQuery("table", singleton("col1"), emptyList()));
    }
}
