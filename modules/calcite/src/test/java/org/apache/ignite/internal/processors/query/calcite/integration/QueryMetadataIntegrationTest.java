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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;

/**
 * Test query metadata.
 */
public class QueryMetadataIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        executeSql("CREATE TABLE tbl (id DECIMAL(10, 2) PRIMARY KEY, val VARCHAR, val2 VARCHAR, t DECIMAL(10, 2))");
        executeSql("CREATE INDEX idx_id_val ON tbl (id DESC, val)");
        executeSql("CREATE INDEX idx_id_val2 ON tbl (id, val2 DESC)");

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO tbl VALUES (?, ?, ?, ?)", i, "val" + i, "val" + i, i);

        executeSql("CREATE TABLE tbl2 (id DECIMAL(10, 2) PRIMARY KEY, val VARCHAR)");

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO tbl2 VALUES (?, ?)", i, "val" + i);
    }

    /** */
    @Test
    public void test() throws Exception {
        List<T2<List<GridQueryFieldMetadata>, List<GridQueryFieldMetadata>>> meta =
            queryEngine(client).queryMetadata(null, "PUBLIC", "select * from tbl where tbl.t > ?");

        List<GridQueryFieldMetadata> resMeta = meta.get(0).getKey();
        List<GridQueryFieldMetadata> paramMeta = meta.get(0).getValue();

        List<List<?>> res = executeSql("select typeof(t) from tbl");
        
        log.warning("Metadata :" + resMeta);
    }

    /** */
    private QueryEngine queryEngine(IgniteEx ign) {
        return Commons.lookupComponent(ign.context(), QueryEngine.class);
    }
}
