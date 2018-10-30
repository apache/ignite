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

package org.apache.ignite.internal.processor.security;

import java.util.Set;
import junit.framework.TestSuite;
import org.jetbrains.annotations.Nullable;

/**
 * Security test suite.
 */
public class SecurityContextResolverSecurityProcessorTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution. Providing null means nothing to exclude.
     * @return Test suite.
     */
    public static TestSuite suite(final @Nullable Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Initiator Node's Security Context Test Suite");

        suite.addTest(new TestSuite(DistributedClosureTest.class));
        suite.addTest(new TestSuite(ComputeTaskTest.class));
        suite.addTest(new TestSuite(ExecuteServiceTaskTest.class));
        suite.addTest(new TestSuite(ScanQueryTest.class));
        suite.addTest(new TestSuite(EntryProcessorTest.class));
        suite.addTest(new TestSuite(IgniteDataStreamerTest.class));
        suite.addTest(new TestSuite(LoadCacheTest.class));

        return suite;
    }

}
