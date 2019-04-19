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

import junit.framework.TestSuite;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Suite for shared memory mode.
 */
@RunWith(AllTests.class)
public class IgniteIpcTestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite IPC Shared Memory Suite");

        if (U.isLinux() || U.isMacOs())
            suite.addTest(IgniteIpcSharedMemorySelfTestSuite.suite());

        return suite;
    }
}
