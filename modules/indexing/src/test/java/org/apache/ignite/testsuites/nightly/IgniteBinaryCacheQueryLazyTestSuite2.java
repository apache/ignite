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

package org.apache.ignite.testsuites.nightly;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testsuites.IgniteBinaryCacheQueryTestSuite2;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries with lazy mode.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteBinaryCacheQueryTestSuite2.class,
})
public class IgniteBinaryCacheQueryLazyTestSuite2 {
    /**
     * Setup lazy mode default.
     */
    @BeforeClass
    public static void setupLazy() {
        GridTestUtils.setFieldValue(SqlFieldsQuery.class, "DFLT_LAZY", true);
    }
}
