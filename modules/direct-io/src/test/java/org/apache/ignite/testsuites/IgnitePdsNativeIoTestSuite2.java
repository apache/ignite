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
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.DiskPageCompressionIntegrationDirectIOTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteNativeIoLocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteNativeIoPdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteNativeIoWalFlushFsyncSelfTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Same as {@link IgnitePdsTestSuite2} but is started with direct-oi jar in classpath.
 */
@RunWith(DynamicSuite.class)
public class IgnitePdsNativeIoTestSuite2 {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        IgnitePdsTestSuite2.addRealPageStoreTests(suite, null);

        // Direct IO + Page compression.
        suite.add(DiskPageCompressionIntegrationDirectIOTest.class);

        //Integrity test with reduced count of pages.
        suite.add(IgniteNativeIoPdsRecoveryAfterFileCorruptionTest.class);

        suite.add(IgniteNativeIoLocalWalModeChangeDuringRebalancingSelfTest.class);

        suite.add(IgniteNativeIoWalFlushFsyncSelfTest.class);

        return suite;
    }
}
