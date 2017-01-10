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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractFieldsQuerySelfTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests for fields queries.
 */
public class IgniteCacheLocalFieldsQuerySelfTest extends IgniteCacheAbstractFieldsQuerySelfTest {
//    static {
//        System.setProperty(IgniteSystemProperties.IGNITE_H2_DEBUG_CONSOLE, "1");
//    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInformationSchema() throws Exception {
        IgniteEx ignite = grid(0);

        ignite.cache(CACHE).query(
            new SqlFieldsQuery("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS").setLocal(true)).getAll();
    }
}