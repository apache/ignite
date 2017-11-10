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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.ZKClusterNodeNew;
import org.apache.ignite.testframework.GridTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 *
 */
public class ZKDisconnectTest2 {
    /** */
    private static final Logger LOG = LoggerFactory.getLogger(ZKDisconnectTest2.class);

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
                    LOG.info("TestClientCnxnSocketNIO block connected");

                    blockConnect.await();

                    LOG.info("TestClientCnxnSocketNIO finish block");
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
            final TestingCluster zkCluster = new TestingCluster(1);
            zkCluster.start();

            Thread.sleep(1000);

            LOG.info("ZK started\n");

            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, TestClientCnxnSocketNIO.class.getName());

            ZKClusterNodeNew node1 = new ZKClusterNodeNew("n1");
            node1.join(zkCluster.getConnectString());

            ZKClusterNodeNew node2 = new ZKClusterNodeNew("n2");
            node2.join(zkCluster.getConnectString());

            LOG.info("Clients connected");

            Thread.sleep(3000);

            LOG.info("Close channel");

            TestClientCnxnSocketNIO.instance.blockConnect = new CountDownLatch(1);
            TestClientCnxnSocketNIO.instance.testClose();

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ZKClusterNodeNew node3 = new ZKClusterNodeNew("n3");
                    node3.join(zkCluster.getConnectString(), 2000);

                    return null;
                }
            }, "start");

            Thread.sleep(3000);

            LOG.info("Stop block");

            TestClientCnxnSocketNIO.instance.blockConnect.countDown();

            fut.get();

            LOG.info("Done");

            Thread.sleep(60_000);
        }
        catch (Throwable e) {
            e.printStackTrace(System.out);

            System.exit(1);
        }
    }
}
