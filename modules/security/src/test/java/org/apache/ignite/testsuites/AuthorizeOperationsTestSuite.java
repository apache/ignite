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

import org.apache.ignite.internal.processor.security.cache.CachePermissionsSecurityTest;
import org.apache.ignite.internal.processor.security.cache.EntryProcessorPermissionSecurityTest;
import org.apache.ignite.internal.processor.security.cache.LoadCachePermissionSecurityTest;
import org.apache.ignite.internal.processor.security.cache.ScanQueryPermissionSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.EntryProcessorCacheResolveSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.LoadCacheResolveSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.ScanQueryCacheResolveSecurityTest;
import org.apache.ignite.internal.processor.security.client.ThinClientSecurityTest;
import org.apache.ignite.internal.processor.security.compute.TaskExecutePermissionTest;
import org.apache.ignite.internal.processor.security.compute.closure.ComputeTaskComputeResolveSecurityTest;
import org.apache.ignite.internal.processor.security.compute.closure.DistributedClosureComputeResolveSecurityTest;
import org.apache.ignite.internal.processor.security.compute.closure.ExecutorServiceComputeResolveSecurityTest;
import org.apache.ignite.internal.processor.security.datastreamer.DataStreamePermissionSecurityTest;
import org.apache.ignite.internal.processor.security.datastreamer.closure.DataStreamerCacheResolveSecurityTest;
import org.apache.ignite.internal.processor.security.messaging.IgniteMessagingResolveSecurityTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Security test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CachePermissionsSecurityTest.class,
    DataStreamePermissionSecurityTest.class,
    ScanQueryPermissionSecurityTest.class,
    LoadCachePermissionSecurityTest.class,
    EntryProcessorPermissionSecurityTest.class,
    TaskExecutePermissionTest.class,

    DistributedClosureComputeResolveSecurityTest.class,
    ComputeTaskComputeResolveSecurityTest.class,
    ExecutorServiceComputeResolveSecurityTest.class,
    ScanQueryCacheResolveSecurityTest.class,
    EntryProcessorCacheResolveSecurityTest.class,
    DataStreamerCacheResolveSecurityTest.class,
    LoadCacheResolveSecurityTest.class,
    ThinClientSecurityTest.class,
    IgniteMessagingResolveSecurityTest.class,
})
public class AuthorizeOperationsTestSuite {
}
