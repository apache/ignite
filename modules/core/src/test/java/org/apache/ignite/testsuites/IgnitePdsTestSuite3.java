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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.EagerTtlTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTestWithExpiryPolicy;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsNodeJoinWithCachesStopping;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite3 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        addRealPageStoreTestsNotForDirectIo(suite, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests list.`
     */
    private static void addRealPageStoreTestsNotForDirectIo(List<Class<?>> suite, Collection<Class> ignoredTests) {
        // Rebalancing test
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsContinuousRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsContinuousRestartTestWithExpiryPolicy.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsNodeJoinWithCachesStopping.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, EagerTtlTest.class, ignoredTests);
    }
}
