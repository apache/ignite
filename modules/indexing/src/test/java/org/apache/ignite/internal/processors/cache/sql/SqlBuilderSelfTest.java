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

import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlBuilder;

/**
 *
 */
public class SqlBuilderSelfTest extends SqlSelfTest {

    public void testBuild() throws Exception {
        startGrids(2);
        createAndFillCache();

        int size = getResultQuerySize(SqlBuilder.create().build(PersonTest.class));
        assertEquals(2, size);
    }

    public void testMore() throws Exception {
        startGrids(2);
        createAndFillCache();

        int size = getResultQuerySize(SqlBuilder.create().more("age",10).build(PersonTest.class));
        assertEquals(2, size);
    }

    public void testEq() throws Exception {
        startGrids(2);
        createAndFillCache();

        int size = getResultQuerySize(SqlBuilder.create().eq("name","one1").build(PersonTest.class));
        assertEquals(1, size);
    }

    public void testNotEq() throws Exception {
        startGrids(2);
        createAndFillCache();

        int size = getResultQuerySize(SqlBuilder.create().notEq("name","one2").build(PersonTest.class));
        assertEquals(1, size);
    }

    @Override
    void initCacheData() {
        cache.put("1", new PersonTest("one1", 100));
        cache.put("2", new PersonTest("one2", 50));
    }
}
