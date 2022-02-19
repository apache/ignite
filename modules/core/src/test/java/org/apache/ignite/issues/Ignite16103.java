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

package org.apache.ignite.issues;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Ignite16103 extends GridCommonAbstractTest {

    /**
     * Ignite instance.
     */
    private Ignite ignite;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        startGrid(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTest() {
        ignite = grid(1);
    }

    /**
     *
     */
    @Test
    public void test() {

        IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        cache.query(new SqlFieldsQuery("create table table(id int PRIMARY KEY, fld1 int, fld2 int) with \"CACHE_NAME=TEST_CACHE_NAME,VALUE_TYPE=TEST_VALUE_TYPE\";")).getAll();

        cache.query(new SqlFieldsQuery("create index idx_0 on table(fld1, fld2) INLINE_SIZE 0;")).getAll();
    }
}
