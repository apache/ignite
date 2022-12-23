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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotCheckWithIndexesTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotMetricsTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotRestoreWithIndexingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotWithIndexesTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteSnapshotWithIndexingTestSuite {
    /** */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        addSnapshotTests(suite, null);

        return suite;
    }

    /** */
    public static void addSnapshotTests(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotWithIndexesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotCheckWithIndexesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotRestoreWithIndexingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotMetricsTest.class, ignoredTests);
    }
}
