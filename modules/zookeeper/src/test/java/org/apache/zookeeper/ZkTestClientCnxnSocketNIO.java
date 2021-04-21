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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.zookeeper.client.ZKClientConfig;

/**
 *
 */
public class ZkTestClientCnxnSocketNIO extends ClientCnxnSocketNIO {
    /** */
    public static final IgniteLogger log = new JavaLogger().getLogger(ZkTestClientCnxnSocketNIO.class);

    /** */
    public static volatile boolean DEBUG = false;

    /** */
    public volatile CountDownLatch blockConnectLatch;

    /** */
    public static ConcurrentHashMap<String, ZkTestClientCnxnSocketNIO> clients = new ConcurrentHashMap<>();

    /** */
    private final String nodeName;

    /**
     *
     */
    public static void reset() {
        clients.clear();
    }

    /**
     * @param node Node.
     * @return ZK client.
     */
    public static ZkTestClientCnxnSocketNIO forNode(Ignite node) {
        return clients.get(node.name());
    }

    /**
     * @param instanceName Ignite instance name.
     * @return ZK client.
     */
    public static ZkTestClientCnxnSocketNIO forNode(String instanceName) {
        return clients.get(instanceName);
    }

    /**
     * @throws IOException If failed.
     */
    public ZkTestClientCnxnSocketNIO(ZKClientConfig clientCfg) throws IOException {
        super(clientCfg);

        String threadName = Thread.currentThread().getName();

        nodeName = threadName.substring(threadName.indexOf('-') + 1);

        if (DEBUG)
            log.info("ZkTestClientCnxnSocketNIO created for node: " + nodeName);
    }

    /** {@inheritDoc} */
    @Override void connect(InetSocketAddress addr) throws IOException {
        CountDownLatch blockConnect = this.blockConnectLatch;

        if (DEBUG)
            log.info("ZkTestClientCnxnSocketNIO connect [node=" + nodeName + ", addr=" + addr + ']');

        if (blockConnect != null && blockConnect.getCount() > 0) {
            try {
                log.info("ZkTestClientCnxnSocketNIO block connect");

                blockConnect.await(60, TimeUnit.SECONDS);

                log.info("ZkTestClientCnxnSocketNIO finish block connect");
            }
            catch (Exception e) {
                log.error("Error in ZkTestClientCnxnSocketNIO: " + e, e);
            }
        }

        super.connect(addr);

        clients.put(nodeName, this);
    }

    /**
     *
     */
    public void allowConnect() {
        if (blockConnectLatch == null || blockConnectLatch.getCount() == 0)
            return;

        log.info("ZkTestClientCnxnSocketNIO allowConnect [node=" + nodeName + ']');

        blockConnectLatch.countDown();
    }

    /**
     * @param blockConnect {@code True} to block client reconnect.
     * @throws Exception If failed.
     */
    public void closeSocket(boolean blockConnect) throws Exception {
        if (blockConnect)
            blockConnectLatch = new CountDownLatch(1);

        log.info("ZkTestClientCnxnSocketNIO closeSocket [node=" + nodeName + ", block=" + blockConnect + ']');

        SelectionKey k = GridTestUtils.getFieldValue(this, ClientCnxnSocketNIO.class, "sockKey");

        k.channel().close();
    }
}
