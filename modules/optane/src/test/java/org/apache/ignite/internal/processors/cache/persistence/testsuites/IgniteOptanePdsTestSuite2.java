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

package org.apache.ignite.internal.processors.cache.persistence.testsuites;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlyWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.mmap.optane.OptaneBuffersTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.apache.ignite.testsuites.IgnitePdsTestSuite2;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteOptanePdsTestSuite2 {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        List<Class<?>> tests = IgnitePdsTestSuite2.suite().stream()
            .filter(c -> !ignoredTests.contains(c))
            .collect(Collectors.toList());

        tests.add(OptaneBuffersTest.class);
        return tests;
    }

    /** Ignored tests */
    private static final Set<Class<?>> ignoredTests = Stream.of(
        IgniteWalFlushBackgroundWithMmapBufferSelfTest.class,
        IgniteWalFlushLogOnlyWithMmapBufferSelfTest.class,
        IgniteWalFlushFsyncWithMmapBufferSelfTest.class,
        IgniteUidAsConsistentIdMigrationTest.class,
        // Always fails https://issues.apache.org/jira/browse/IGNITE-10652
        LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest.class
    ).collect(Collectors.toSet());
}
