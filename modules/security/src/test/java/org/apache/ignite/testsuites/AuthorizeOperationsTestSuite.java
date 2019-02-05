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

import org.apache.ignite.internal.processor.security.cache.CachePermissionsTest;
import org.apache.ignite.internal.processor.security.cache.EntryProcessorCachePermissionTest;
import org.apache.ignite.internal.processor.security.cache.LoadCachePermissionTest;
import org.apache.ignite.internal.processor.security.cache.ScanQueryCachePermissionTest;
import org.apache.ignite.internal.processor.security.cache.closure.EntryProcessorCacheSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.LoadCacheSecurityTest;
import org.apache.ignite.internal.processor.security.cache.closure.ScanQueryCacheSecurityTest;
import org.apache.ignite.internal.processor.security.client.ThinClientSecurityTest;
import org.apache.ignite.internal.processor.security.compute.ComputeTaskExecutePermissionTest;
import org.apache.ignite.internal.processor.security.compute.DistributedClosureExecutePermissionTest;
import org.apache.ignite.internal.processor.security.compute.ExecutorServiceExecutePermissionTest;
import org.apache.ignite.internal.processor.security.compute.closure.ComputeTaskSecurityTest;
import org.apache.ignite.internal.processor.security.compute.closure.DistributedClosureComputeTaskSecurityTest;
import org.apache.ignite.internal.processor.security.compute.closure.ExecutorServiceComputeTaskSecurityTest;
import org.apache.ignite.internal.processor.security.datastreamer.DataStreamerCachePermissionTest;
import org.apache.ignite.internal.processor.security.datastreamer.closure.IgniteDataStreamerCacheSecurityTest;
import org.apache.ignite.internal.processor.security.messaging.IgniteMessagingResolveSecurityContextTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Security test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CachePermissionsTest.class,
    DataStreamerCachePermissionTest.class,
    ScanQueryCachePermissionTest.class,
    LoadCachePermissionTest.class,
    EntryProcessorCachePermissionTest.class,
    ExecutorServiceExecutePermissionTest.class,
    DistributedClosureExecutePermissionTest.class,
    ComputeTaskExecutePermissionTest.class,

    DistributedClosureComputeTaskSecurityTest.class,
    ComputeTaskSecurityTest.class,
    ExecutorServiceComputeTaskSecurityTest.class,
    ScanQueryCacheSecurityTest.class,
    EntryProcessorCacheSecurityTest.class,
    IgniteDataStreamerCacheSecurityTest.class,
    LoadCacheSecurityTest.class,
    ThinClientSecurityTest.class,
    IgniteMessagingResolveSecurityContextTest.class,
})
public class AuthorizeOperationsTestSuite {
}
