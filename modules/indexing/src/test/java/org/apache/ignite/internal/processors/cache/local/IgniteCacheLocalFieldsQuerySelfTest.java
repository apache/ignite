/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests for fields queries.
 */
@RunWith(JUnit4.class)
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
    @Test
    public void testInformationSchema() throws Exception {
        jcache(String.class, String.class).query(
            new SqlFieldsQuery("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS").setLocal(true)).getAll();
    }
}
