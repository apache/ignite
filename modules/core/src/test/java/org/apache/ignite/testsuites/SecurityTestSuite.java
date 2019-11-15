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

import org.apache.ignite.internal.processors.security.InvalidServerTest;
import org.apache.ignite.internal.processors.security.cache.CacheOperationPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.EntryProcessorPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.ScanQueryPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.CacheLoadRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.EntryProcessorRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.ScanQueryRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.client.ThinClientPermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.ComputePermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ComputeTaskCancelRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ComputeTaskRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.DistributedClosureRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ExecutorServiceRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.DataStreamerPermissionCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.closure.DataStreamerRemoteSecurityContextCheckTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Security test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CacheOperationPermissionCheckTest.class,
    DataStreamerPermissionCheckTest.class,
    ScanQueryPermissionCheckTest.class,
    EntryProcessorPermissionCheckTest.class,
    ComputePermissionCheckTest.class,

    DistributedClosureRemoteSecurityContextCheckTest.class,
    ComputeTaskRemoteSecurityContextCheckTest.class,
    ComputeTaskCancelRemoteSecurityContextCheckTest.class,
    ExecutorServiceRemoteSecurityContextCheckTest.class,
    ScanQueryRemoteSecurityContextCheckTest.class,
    EntryProcessorRemoteSecurityContextCheckTest.class,
    DataStreamerRemoteSecurityContextCheckTest.class,
    CacheLoadRemoteSecurityContextCheckTest.class,
    ThinClientPermissionCheckTest.class,

    InvalidServerTest.class,
})
public class SecurityTestSuite {
}
