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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class PlatformServiceTestUtils {
    private static String SVC_1 = "Service1";

    /** */
    private static volatile IgniteEx client;

    /** */
    public static void start() {
        if (client == null)
            synchronized (PlatformServiceTestUtils.class) {
                if (client == null)
                    client = (IgniteEx) Ignition.start(clientConfiguration());
            }
    }

    /** */
    public static void testCall() {
        IgniteEx client0 = client;

        if (client0 == null)
            throw new RuntimeException("Client must be initialized");

        TestPlatformService svc = client0.services().serviceProxy(SVC_1, TestPlatformService.class, false);

        try {
            svc.get_NodeId();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException("Invocation failed with error", e);
        }
    }

    /** */
    public static void stop() {
        if (client != null)
            synchronized (PlatformServiceTestUtils.class) {
                if (client != null) {
                    Ignition.stop(client.name(), true);

                    client = null;
                }
            }
    }

    /** */
    private static IgniteConfiguration clientConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("java-client");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500"));

        disco.setIpFinder(ipFinder);
        disco.setNetworkTimeout(3000);

        cfg.setDiscoverySpi(disco);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(true);
        //bCfg.setNameMapper(new BinaryBasicNameMapper().setSimpleName(true));
        bCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration().setTypeName(PlatformComputeBinarizable.class.getName()),
            new BinaryTypeConfiguration().setTypeName(PlatformComputeJavaBinarizable.class.getName()),
            new BinaryTypeConfiguration().setTypeName(PlatformComputeEnum.class.getName()).setEnum(true)
        ));

        cfg.setBinaryConfiguration(bCfg);

        cfg.setClientMode(true);

        return cfg;
    }

    /** */
    public static interface TestPlatformService {
        /** */
        public boolean get_Initialized();

        /** */
        public boolean get_Cancelled();

        /** */
        public boolean get_Executed();

        /** */
        public UUID get_NodeId();

        /** */
        public String get_LastCallContextName();

        /** */
        public Object ErrorMethod(Object arg);
    }
}
