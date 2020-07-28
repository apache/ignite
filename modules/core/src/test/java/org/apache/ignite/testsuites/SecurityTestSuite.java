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

import org.apache.ignite.internal.processors.security.GridCommandHandlerSslWithSecurityTest;
import org.apache.ignite.internal.processors.security.IgniteSecurityProcessorTest;
import org.apache.ignite.internal.processors.security.InvalidServerTest;
import org.apache.ignite.internal.processors.security.cache.CacheOperationPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.CacheOperationPermissionCreateDestroyCheckTest;
import org.apache.ignite.internal.processors.security.cache.ContinuousQueryPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.EntryProcessorPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.ScanQueryPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.CacheLoadRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.ContinuousQueryRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.ContinuousQueryWithTransformerRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.EntryProcessorRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.ScanQueryRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.client.AdditionalSecurityCheckTest;
import org.apache.ignite.internal.processors.security.client.AdditionalSecurityCheckWithGlobalAuthTest;
import org.apache.ignite.internal.processors.security.client.AttributeSecurityCheckTest;
import org.apache.ignite.internal.processors.security.client.ThinClientPermissionCheckSecurityTest;
import org.apache.ignite.internal.processors.security.client.ThinClientPermissionCheckTest;
import org.apache.ignite.internal.processors.security.client.ThinClientSecurityContextOnRemoteNodeTest;
import org.apache.ignite.internal.processors.security.client.ThinClientSslPermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.ComputePermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ComputeTaskCancelRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ComputeTaskRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.DistributedClosureRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ExecutorServiceRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.DataStreamerPermissionCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.closure.DataStreamerRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.events.EventsRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.messaging.MessagingRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.sandbox.AccessToClassesInsideInternalPackageTest;
import org.apache.ignite.internal.processors.security.sandbox.CacheSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.ComputeSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.ContinuousQuerySandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.ContinuousQueryWithTransformerSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.DataStreamerSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.DoPrivilegedOnRemoteNodeTest;
import org.apache.ignite.internal.processors.security.sandbox.EventsSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.IgniteOperationsInsideSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.IgnitionComponentProxyTest;
import org.apache.ignite.internal.processors.security.sandbox.MessagingSandboxTest;
import org.apache.ignite.internal.processors.security.sandbox.PrivilegedProxyTest;
import org.apache.ignite.internal.processors.security.sandbox.SecuritySubjectPermissionsTest;
import org.apache.ignite.ssl.MultipleSSLContextsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Security test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CacheOperationPermissionCheckTest.class,
    CacheOperationPermissionCreateDestroyCheckTest.class,
    DataStreamerPermissionCheckTest.class,
    ScanQueryPermissionCheckTest.class,
    EntryProcessorPermissionCheckTest.class,
    ComputePermissionCheckTest.class,
    ThinClientPermissionCheckTest.class,
    ThinClientPermissionCheckSecurityTest.class,
    ContinuousQueryPermissionCheckTest.class,

    DistributedClosureRemoteSecurityContextCheckTest.class,
    ComputeTaskRemoteSecurityContextCheckTest.class,
    ComputeTaskCancelRemoteSecurityContextCheckTest.class,
    ExecutorServiceRemoteSecurityContextCheckTest.class,
    ScanQueryRemoteSecurityContextCheckTest.class,
    EntryProcessorRemoteSecurityContextCheckTest.class,
    DataStreamerRemoteSecurityContextCheckTest.class,
    CacheLoadRemoteSecurityContextCheckTest.class,
    ContinuousQueryRemoteSecurityContextCheckTest.class,
    ContinuousQueryWithTransformerRemoteSecurityContextCheckTest.class,
    ThinClientSslPermissionCheckTest.class,
    ThinClientSecurityContextOnRemoteNodeTest.class,
    MessagingRemoteSecurityContextCheckTest.class,
    EventsRemoteSecurityContextCheckTest.class,

    InvalidServerTest.class,
    AdditionalSecurityCheckTest.class,
    AttributeSecurityCheckTest.class,
    AdditionalSecurityCheckWithGlobalAuthTest.class,

    CacheSandboxTest.class,
    DataStreamerSandboxTest.class,
    ComputeSandboxTest.class,
    DoPrivilegedOnRemoteNodeTest.class,
    IgniteOperationsInsideSandboxTest.class,
    AccessToClassesInsideInternalPackageTest.class,
    SecuritySubjectPermissionsTest.class,
    IgnitionComponentProxyTest.class,
    MessagingSandboxTest.class,
    ContinuousQuerySandboxTest.class,
    ContinuousQueryWithTransformerSandboxTest.class,
    EventsSandboxTest.class,
    PrivilegedProxyTest.class,

    IgniteSecurityProcessorTest.class,
    GridCommandHandlerSslWithSecurityTest.class,
    MultipleSSLContextsTest.class
})
public class SecurityTestSuite {
}
