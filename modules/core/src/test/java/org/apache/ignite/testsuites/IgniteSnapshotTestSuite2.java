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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotHandlerTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotRestoreSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotMXBeanTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteSnapshotTestSuite2 {
    /** */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        addSnapshotTests1(suite, null);
        addSnapshotTests2(suite, null);

        return suite;
    }

    /** */
    public static void addSnapshotTests1(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotCheckTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotHandlerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteSnapshotMXBeanTest.class, ignoredTests);
    }

    /** */
    public static void addSnapshotTests2(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterSnapshotRestoreSelfTest.class, ignoredTests);
    }
}
