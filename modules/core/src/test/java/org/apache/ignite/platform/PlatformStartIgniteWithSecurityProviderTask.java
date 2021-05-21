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

package org.apache.ignite.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Task to start an Ignite node.
 */
public class PlatformStartIgniteWithSecurityProviderTask extends ComputeTaskAdapter<String, String> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String arg) throws IgniteException {
        return Collections.singletonMap(new PlatformStartIgniteJob(arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
        ComputeJobResult res = results.get(0);

        if (res.getException() != null)
            throw res.getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class PlatformStartIgniteJob extends ComputeJobAdapter {
        /** */
        private final String instanceName;

        /**
         * Ctor.
         *
         * @param instanceName Isntance name.
         */
        private PlatformStartIgniteJob(String instanceName) {
            this.instanceName = instanceName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            TestSecurityPluginProvider securityPluginProvider = new TestSecurityPluginProvider(
                    "login1",
                    "pass1",
                    ALLOW_ALL,
                    false,
                    new TestSecurityData("CLIENT",
                            SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                                    .appendCachePermissions("DEFAULT_CACHE", CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                                    .appendCachePermissions("FORBIDDEN_CACHE")
                                    .build())
            );

            IgniteConfiguration cfg = new IgniteConfiguration()
                    .setPluginProviders(securityPluginProvider)
                    .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(GridAbstractTest.LOCAL_IP_FINDER))
                    .setLocalHost("127.0.0.1")
                    .setIgniteInstanceName(instanceName);

            Ignite ignite = Ignition.start(cfg);

            return ignite.name();
        }
    }
}
