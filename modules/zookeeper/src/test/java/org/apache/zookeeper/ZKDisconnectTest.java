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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.ZKClusterNode;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 *
 */
public class ZKDisconnectTest {
    public static class TestClientCnxnSocketNIO extends ClientCnxnSocketNIO {
        private static TestClientCnxnSocketNIO instance;

        volatile CountDownLatch blockConnect;

        public TestClientCnxnSocketNIO() throws IOException {
            super();

            if (instance == null)
                instance = this;
        }

        @Override
        void connect(InetSocketAddress addr) throws IOException {
            System.out.println("TestClientCnxnSocketNIO connect: " + addr);

            CountDownLatch blockConnect = this.blockConnect;

            if (blockConnect != null) {
                try {
                    System.out.println("TestClientCnxnSocketNIO block connected");

                    blockConnect.await();

                    System.out.println("TestClientCnxnSocketNIO finish block");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                this.blockConnect = null;
            }

            super.connect(addr);
        }

        void testClose() {
            try {
                SelectionKey k = GridTestUtils.getFieldValue(this, ClientCnxnSocketNIO.class, "sockKey");

                k.channel().close();
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            TestingCluster zkCluster = new TestingCluster(1);
            zkCluster.start();

            Thread.sleep(1000);

            System.out.println("ZK started\n");

            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, TestClientCnxnSocketNIO.class.getName());

            ZKClusterNode node1 = new ZKClusterNode("n1");
            node1.join(zkCluster.getConnectString());

            ZKClusterNode node2 = new ZKClusterNode("n2");
            node2.join(zkCluster.getConnectString());

            System.out.println("Client connected");

            Thread.sleep(1000);

            System.out.println("Close channel");

            TestClientCnxnSocketNIO.instance.blockConnect = new CountDownLatch(1);
            TestClientCnxnSocketNIO.instance.testClose();

            System.out.println("Closed");

            ZKClusterNode node3 = new ZKClusterNode("n3");
            node3.join(zkCluster.getConnectString());

            System.out.println("Node started");

            node3.stop();

            ZKClusterNode node4 = new ZKClusterNode("n4");
            node4.join(zkCluster.getConnectString());

            System.out.println("Node stopped");

            TestClientCnxnSocketNIO.instance.blockConnect.countDown();

            Thread.sleep(60_000);
        }
        catch (Throwable e) {
            e.printStackTrace(System.out);

            System.exit(1);
        }
    }
}
