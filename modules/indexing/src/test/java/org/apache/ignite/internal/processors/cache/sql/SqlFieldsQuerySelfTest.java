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

package org.apache.ignite.internal.processors.cache.sql;

import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 *
 */
public class SqlFieldsQuerySelfTest extends SqlSelfTest {

    private static final SqlFieldsQuery DEFAULT_QUERY = new SqlFieldsQuery("select name, age from PersonTest where age > 10");

    /**
     * @throws Exception If error.
     */
    public void testSqlFieldsQuery() throws Exception {
        startGrids(2);
        createAndFillCache();

        int size = getResultQuerySize(DEFAULT_QUERY);
        assertEquals(2, size);
    }

    /**
     * @throws Exception If error.
     */
    public void testSqlFieldsQueryWithTopologyChanges() throws Exception {
        startGrid(0);
        createAndFillCache();
        startGrid(1);

        int size = getResultQuerySize(DEFAULT_QUERY);
        assertEquals(2, size);
    }

    @Override
    void initCacheData() {
        cache.put("1", new PersonTest("sun", 100));
        cache.put("2", new PersonTest("moon", 50));
    }
}
