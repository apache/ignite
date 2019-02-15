/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
 */

package org.apache.ignite.stream.rocketmq;

import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import static java.io.File.separator;

/**
 * Test RocketMQ server handling a broker and a nameserver.
 */
class TestRocketMQServer {
    /** Nameserver port. */
    protected static final int NAME_SERVER_PORT = 9000;

    /** Broker port. */
    private static final int BROKER_PORT = 8000;

    /** Broker HA port. */
    private static final int HA_PORT = 8001;

    /** Test ip address. */
    protected static final String TEST_IP = "127.0.0.1";

    /** Test broker name. */
    private static final String TEST_BROKER = "testBroker";

    /** Test cluster name. */
    private static final String TEST_CLUSTER = "testCluster";

    /** Nameserver. */
    private static NamesrvController nameSrv;

    /** Broker. */
    private static BrokerController broker;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Test server constructor.
     *
     * @param log Logger.
     */
    TestRocketMQServer(IgniteLogger log) {
        this.log = log;

        try {
            startNameServer();
            startBroker();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start RocketMQ: " + e);
        }
    }

    /**
     * Starts a test nameserver.
     *
     * @throws Exception If fails.
     */
    private void startNameServer() throws Exception {
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nameServerNettyServerConfig = new NettyServerConfig();

        namesrvConfig.setKvConfigPath(System.getProperty("java.io.tmpdir") + separator + "namesrv" + separator + "kvConfig.json");
        nameServerNettyServerConfig.setListenPort(NAME_SERVER_PORT);

        nameSrv = new NamesrvController(namesrvConfig, nameServerNettyServerConfig);

        nameSrv.initialize();
        nameSrv.start();

        log.info("Started nameserver at " + NAME_SERVER_PORT);
    }

    /**
     * Starts a test broker.
     *
     * @throws Exception If fails.
     */
    private void startBroker() throws Exception {
        BrokerConfig brokerCfg = new BrokerConfig();
        NettyServerConfig nettySrvCfg = new NettyServerConfig();
        MessageStoreConfig storeCfg = new MessageStoreConfig();

        brokerCfg.setBrokerName(TEST_BROKER);
        brokerCfg.setBrokerClusterName(TEST_CLUSTER);
        brokerCfg.setBrokerIP1(TEST_IP);
        brokerCfg.setNamesrvAddr(TEST_IP + ":" + NAME_SERVER_PORT);

        storeCfg.setStorePathRootDir(System.getProperty("java.io.tmpdir") + separator + "store-" + UUID.randomUUID());
        storeCfg.setStorePathCommitLog(System.getProperty("java.io.tmpdir") + separator + "commitlog");
        storeCfg.setHaListenPort(HA_PORT);

        nettySrvCfg.setListenPort(BROKER_PORT);

        broker = new BrokerController(brokerCfg, nettySrvCfg, new NettyClientConfig(), storeCfg);

        broker.initialize();
        broker.start();

        log.info("Started broker [" + TEST_BROKER + "] at " + BROKER_PORT);
    }

    /**
     * Obtains the broker address.
     *
     * @return Broker address.
     */
    String getBrokerAddr() {
        return broker.getBrokerAddr();
    }

    /**
     * Shuts test server down.
     */
    void shutdown() {
        if (broker != null)
            broker.shutdown();

        if (nameSrv != null)
            nameSrv.shutdown();
    }
}
