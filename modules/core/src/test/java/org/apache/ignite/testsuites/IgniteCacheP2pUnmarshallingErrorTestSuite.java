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

import junit.framework.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.testframework.*;

import java.util.*;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingErrorTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("P2p Unmarshalling Test Suite");

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheP2pUnmarshallingErrorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheP2pUnmarshallingNearErrorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheP2pUnmarshallingRebalanceErrorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheP2pUnmarshallingTxErrorTest.class, ignoredTests);

        return suite;
    }
}
