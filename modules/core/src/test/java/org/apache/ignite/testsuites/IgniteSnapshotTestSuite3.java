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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.BufferedFileIOTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.IgniteCacheDumpFilterTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.IgniteCacheDumpSelf2Test;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.IgniteCacheDumpSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.IgniteConcurrentCacheDumpTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteSnapshotTestSuite3 {
    /** */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        addSnapshotTests(suite, null);

        return suite;
    }

    /** */
    public static void addSnapshotTests(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheDumpSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheDumpSelf2Test.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteConcurrentCacheDumpTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheDumpFilterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, BufferedFileIOTest.class, ignoredTests);
    }
}
