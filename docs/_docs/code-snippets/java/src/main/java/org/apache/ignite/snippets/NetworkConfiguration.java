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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.jupiter.api.Test;

public class NetworkConfiguration {

    @Test
    void discoveryConfigExample() {
        //tag::discovery[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi().setLocalPort(8300);

        cfg.setDiscoverySpi(discoverySpi);
        Ignite ignite = Ignition.start(cfg);
        //end::discovery[]
        ignite.close();
    }

    @Test
    void failureDetectionTimeout() {
        //tag::failure-detection-timeout[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setFailureDetectionTimeout(5_000);

        cfg.setClientFailureDetectionTimeout(10_000);
        //end::failure-detection-timeout[]
        Ignition.start(cfg).close();
    }

}
