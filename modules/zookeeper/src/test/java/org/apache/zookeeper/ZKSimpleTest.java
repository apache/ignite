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

package org.apache.zookeeper;

import org.apache.curator.test.TestingCluster;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.ZKClusterNodeNew;

/**
 *
 */
public class ZKSimpleTest {
    public static void main(String[] args) throws Exception {
        TestingCluster zkCluster = new TestingCluster(1);
        zkCluster.start();

        System.out.println("ZK started\n");

        ZKClusterNodeNew node1 = new ZKClusterNodeNew("n1");
        node1.join(zkCluster.getConnectString());

        ZKClusterNodeNew node2 = new ZKClusterNodeNew("n2");
        node2.join(zkCluster.getConnectString());

        ZKClusterNodeNew node3 = new ZKClusterNodeNew("n3");
        node3.join(zkCluster.getConnectString());

//        ZKClusterNodeNew node4 = new ZKClusterNodeNew("n4");
//        node4.join(zkCluster.getConnectString());

        System.out.println("Stop n2");

        node2.stop();

        //Thread.sleep(5000);

        System.out.println("Stop n3");

        node3.stop();

        System.out.println("Done");

        Thread.sleep(60_000);
    }
}
