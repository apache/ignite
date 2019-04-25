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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedIteratorsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedIteratorsSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalIteratorsSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Cache iterators test suite.
 */
public class IgniteCacheIteratorsSelfTestSuite {
    /**
     * @param ignoredTests Ignored tests.
     * @return Cache iterators test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalIteratorsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedIteratorsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedIteratorsSelfTest.class, ignoredTests);

        return suite;
   }
}
