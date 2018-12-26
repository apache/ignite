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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processor.security.cache.CachePermissionsTest;
import org.apache.ignite.internal.processor.security.cache.EntryProcessorCachePermissionTest;
import org.apache.ignite.internal.processor.security.cache.LoadCachePermissionTest;
import org.apache.ignite.internal.processor.security.cache.ScanQueryCachePermissionTest;
import org.apache.ignite.internal.processor.security.cache.closure.EntryProcessorSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.LoadCacheSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.ScanQuerySecurityTest;
import org.apache.ignite.internal.processor.security.client.ThinClientSecurityTest;
import org.apache.ignite.internal.processor.security.compute.TaskExecutePermissionForComputeTaskTest;
import org.apache.ignite.internal.processor.security.compute.TaskExecutePermissionForDistributedClosureTest;
import org.apache.ignite.internal.processor.security.compute.TaskExecutePermissionForExecutorServiceTest;
import org.apache.ignite.internal.processor.security.compute.closure.ComputeTaskSecurityTest;
import org.apache.ignite.internal.processor.security.compute.closure.DistributedClosureSecurityTest;
import org.apache.ignite.internal.processor.security.compute.closure.ExecutorServiceTaskSecurityTest;
import org.apache.ignite.internal.processor.security.datastreamer.DataStreamerCachePermissionTest;
import org.apache.ignite.internal.processor.security.datastreamer.closure.IgniteDataStreamerSecurityTest;
import org.apache.ignite.internal.processor.security.messaging.IgniteMessagingTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Security test suite.
 */
@RunWith(AllTests.class)
public class AuthorizeOperationsTestSuite {
    /** */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite(AuthorizeOperationsTestSuite.class.getName());

        suite.addTest(new JUnit4TestAdapter(CachePermissionsTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStreamerCachePermissionTest.class));
        suite.addTest(new JUnit4TestAdapter(ScanQueryCachePermissionTest.class));
        suite.addTest(new JUnit4TestAdapter(LoadCachePermissionTest.class));
        suite.addTest(new JUnit4TestAdapter(EntryProcessorCachePermissionTest.class));
        suite.addTest(new JUnit4TestAdapter(TaskExecutePermissionForExecutorServiceTest.class));
        suite.addTest(new JUnit4TestAdapter(TaskExecutePermissionForDistributedClosureTest.class));
        suite.addTest(new JUnit4TestAdapter(TaskExecutePermissionForComputeTaskTest.class));

        suite.addTest(new JUnit4TestAdapter(DistributedClosureSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(ComputeTaskSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(ExecutorServiceTaskSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(ScanQuerySecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(EntryProcessorSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDataStreamerSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(LoadCacheSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(ThinClientSecurityTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteMessagingTest.class));

        return suite;
    }

}
