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

import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processor.security.cache.ClientNodeCachePermissionsTest;
import org.apache.ignite.internal.processor.security.cache.EntryProcessorSecurityTest;
import org.apache.ignite.internal.processor.security.cache.ServerNodeCachePermissionsTest;
import org.apache.ignite.internal.processor.security.datastreamer.IgniteDataStreamerSecurityTest;
import org.apache.ignite.internal.processor.security.cache.LoadCacheSecurityTest;
import org.apache.ignite.internal.processor.security.cache.ScanQuerySecurityTest;
import org.apache.ignite.internal.processor.security.client.ThinClientSecurityTest;
import org.apache.ignite.internal.processor.security.compute.ComputeTaskSecurityTest;
import org.apache.ignite.internal.processor.security.compute.DistributedClosureSecurityTest;
import org.apache.ignite.internal.processor.security.compute.ExecuteServiceTaskSecurityTest;
import org.apache.ignite.internal.processor.security.messaging.IgniteMessagingTest;
import org.jetbrains.annotations.Nullable;

/**
 * Security test suite.
 */
public class AuthorizeOperationsTestSuite extends TestSuite {
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

        suite.addTest(new TestSuite(DistributedClosureSecurityTest.class));
        suite.addTest(new TestSuite(ComputeTaskSecurityTest.class));
        suite.addTest(new TestSuite(ExecuteServiceTaskSecurityTest.class));
        suite.addTest(new TestSuite(ScanQuerySecurityTest.class));
        suite.addTest(new TestSuite(EntryProcessorSecurityTest.class));
        suite.addTest(new TestSuite(IgniteDataStreamerSecurityTest.class));
        suite.addTest(new TestSuite(LoadCacheSecurityTest.class));
        suite.addTest(new TestSuite(ThinClientSecurityTest.class));
        suite.addTest(new TestSuite(IgniteMessagingTest.class));
        suite.addTest(new TestSuite(ClientNodeCachePermissionsTest.class));
        suite.addTest(new TestSuite(ServerNodeCachePermissionsTest.class));

        return suite;
    }

}
