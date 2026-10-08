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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryClientTopologyGapTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Split off from {@link IgniteSpiDiscoverySelfTestSuite} to reduce the single-suite runtime in CI;
 * contains an independent subset of the same test classes.
 */
@RunWith(DynamicSuite.class)
public class IgniteSpiDiscoverySelfTestSuite2 {
    /** */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        // TODO: temporary, debugging flaky TcpDiscoveryClientTopologyGapTest on TC.
        for (int i = 0; i < 20; i++)
            suite.add(TcpDiscoveryClientTopologyGapTest.class);

        return suite;
    }
}
