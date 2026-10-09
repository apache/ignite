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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_USE_BINARY_ARRAYS;

/** */
public class ComputeCacheCollectionsP2PTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_CLS_NAME =
        "org.apache.ignite.tests.p2p.startcache.PojoCollectionComputeTest";

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_USE_BINARY_ARRAYS, value = "true")
    public void testBinaryArray() throws Exception {
        runTest("PojoArrayTask");
    }

    /** */
    @Test
    public void testArrayList() throws Exception {
        runTest("PojoListTask");
    }

    /** */
    @Test
    public void testHashMap() throws Exception {
        runTest("PojoMapTask");
    }

    /** */
    private void runTest(String taskName) throws Exception {
        try (Ignite ignore = Ignition.start(createConfiguration())) {
            Collection<String> jvmArgs = Arrays.asList("-ea", "-DIGNITE_QUIET=false");
            String cp = U.getIgniteHome() + "/modules/extdata/p2p/target/classes/";

            GridJavaProcess clientNode = GridJavaProcess.exec(
                CLIENT_CLS_NAME,
                taskName,
                log,
                null,
                null,
                null,
                jvmArgs,
                cp
            );

            int exitCode = clientNode.getProcess().waitFor();

            assertEquals("Unexpected exit code", 0, exitCode);
        }
    }

    /** */
    private IgniteConfiguration createConfiguration() {
        return new IgniteConfiguration()
            .setPeerClassLoadingEnabled(true)
            .setLocalHost("127.0.0.1")
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryVmIpFinder()
                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                ));
    }
}
