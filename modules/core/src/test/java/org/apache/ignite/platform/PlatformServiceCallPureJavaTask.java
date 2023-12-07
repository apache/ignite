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
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Test invoke {@link PlatformService} methods with collections and arrays as arguments and return type from
 * pure java node.
 */
public class PlatformServiceCallPureJavaTask extends AbstractPlatformServiceCallTask {
    /** {@inheritDoc} */
    @Override ComputeJobAdapter createJob(String svcName) {
        return new PlatformServiceCallPureJavaTask.PlatformServiceCallPureJavaJob(svcName);
    }

    /** */
    static class PlatformServiceCallPureJavaJob extends
        PlatformServiceCallCollectionsTask.PlatformServiceCallCollectionsJob {
        /** Pure java node. */
        Ignite pureJavaIgnite;

        /**
         * @param srvcName Service name.
         */
        PlatformServiceCallPureJavaJob(String srvcName) {
            super(srvcName);
        }

        /** {@inheritDoc} */
        @Override TestPlatformService serviceProxy() {
            return pureJavaIgnite.services().serviceProxy(srvcName, TestPlatformService.class, false);
        }

        /** {@inheritDoc} */
        @Override void runTest() {
            IgniteConfiguration currCfg = ignite.configuration();
            TcpDiscoverySpi currSpi = (TcpDiscoverySpi)currCfg.getDiscoverySpi();
            TcpDiscoveryVmIpFinder currIpFinder = (TcpDiscoveryVmIpFinder)currSpi.getIpFinder();

            pureJavaIgnite = Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("pure-java-node")
                .setDiscoverySpi(new TcpDiscoverySpi()
                    .setIpFinder(currIpFinder)
                    .setLocalAddress(currSpi.getLocalAddress())
                    .setLocalPortRange(currSpi.getLocalPortRange())
                )
                .setLocalHost(currCfg.getLocalHost())
                .setBinaryConfiguration(currCfg.getBinaryConfiguration())
            );

            try {
                super.runTest();
            }
            finally {
                U.close(pureJavaIgnite, ignite.log().getLogger(getClass()));
            }
        }
    }
}
