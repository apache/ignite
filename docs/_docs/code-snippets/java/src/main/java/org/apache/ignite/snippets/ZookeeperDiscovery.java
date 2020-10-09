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
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

public class ZookeeperDiscovery {

    void ZookeeperDiscoveryConfigurationExample() {
        //tag::cfg[]
        ZookeeperDiscoverySpi zkDiscoverySpi = new ZookeeperDiscoverySpi();

        zkDiscoverySpi.setZkConnectionString("127.0.0.1:34076,127.0.0.1:43310,127.0.0.1:36745");
        zkDiscoverySpi.setSessionTimeout(30_000);

        zkDiscoverySpi.setZkRootPath("/ignite");
        zkDiscoverySpi.setJoinTimeout(10_000);

        IgniteConfiguration cfg = new IgniteConfiguration();

        //Override default discovery SPI.
        cfg.setDiscoverySpi(zkDiscoverySpi);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::cfg[]
        ignite.close();
    }
}
