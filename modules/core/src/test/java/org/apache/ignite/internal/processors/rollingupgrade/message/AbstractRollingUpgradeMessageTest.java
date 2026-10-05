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

package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.MessagesPluginProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.resolveTestMessageClasses;

/**
 * Test messages declare a class per release that changes them. Features below the lowest one of a release are retired.
 * {@code +N} is a field introduced by feature N, {@code -N} is a field deprecated by feature N.
 * <pre>
 * Core    Features  TestCoreMessage
 * 2.18.0  0         A B C
 * 2.19.0  0-1       A B C D+1
 * 2.19.2  0-2       A B C-2 D+1
 * 2.19.3  0-2,6     A B C-2 D+1 F+6
 * 2.20.0  2-5       A B-3 C-2 D-5 E+4
 * 2.20.1  2-6       A B-3 C-2 D-5 E+4 F+6
 * 2.21.0  6         A E F+6
 *
 * Plugin  Features  TestPluginMessage
 * 0.9.0   none      A B C
 * 1.0.0   0         A B C D+0
 * 1.1.0   0-1       A B-1 C D+0
 * 2.0.0   1-3       A B-1 C D-3 E+2
 * 2.1.0   1-4       A B-1 C D-3 E+2 F+4
 * 3.0.0   4         A C E F+4
 * </pre>
 * 2.19.3 carries feature 6 cherry-picked from 2.20.1, so it can upgrade to 2.20.1 but not to 2.20.0.
 * Core D and plugin D live through the whole cycle: introduced, deprecated once the introducing feature is retired, deleted once
 * the deprecating feature is retired.
 */
public abstract class AbstractRollingUpgradeMessageTest extends AbstractRollingUpgradeTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String cmpVers) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, cmpVers);

        cfg.setPluginProviders(F.concat(cfg.getPluginProviders(), new MessagesPluginProvider(resolveTestMessageClasses(cmpVers))));

        return cfg;
    }

    /** */
    protected void startServerNodes(String... vers) throws Exception {
        IgniteEx first = startGrid(0, vers[0]);

        if (Arrays.stream(vers).distinct().count() > 1)
            ru(first).enableVersionUpgrade();

        for (int idx = 1; idx < vers.length; idx++)
            startGrid(idx, vers[idx]);
    }

    /** */
    protected <T extends Message> T send(IgniteEx from, IgniteEx to, T msg) throws Exception {
        AtomicReference<T> got = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        String topic = msg.getClass().getName();

        GridMessageListener lsnr = (nodeId, rcvd, plc) -> {
            got.set((T)rcvd);

            latch.countDown();
        };

        to.context().io().addMessageListener(topic, lsnr);

        try {
            ClusterNode rcvNode = from.context().discovery().node(to.localNode().id());

            from.context().io().sendToCustomTopic(rcvNode, topic, msg, PUBLIC_POOL);

            assertTrue(latch.await(getTestTimeout(), MILLISECONDS));

            return got.get();
        }
        finally {
            to.context().io().removeMessageListener(topic, lsnr);
        }
    }

    /** */
    protected TestMessage send(IgniteEx from, IgniteEx to, TestMessageType msgType) throws Exception {
        return send(from, to, buildMessage(from, msgType));
    }

    /** */
    protected Map<String, TestDiscoveryMessage> sendOverDiscovery(IgniteEx from, TestDiscoveryMessage msg) throws Exception {
        List<Ignite> clusterNodes = Ignition.allGrids();

        Map<String, TestDiscoveryMessage> receivedMsgs = new ConcurrentHashMap<>();

        CountDownLatch latch = new CountDownLatch(clusterNodes.size());

        for (Ignite rcv : clusterNodes) {
            ((IgniteEx)rcv).context().discovery().setCustomEventListener(TestDiscoveryMessage.class, (v, n, m) -> {
                receivedMsgs.put(rcv.name(), m);

                latch.countDown();
            });
        }

        from.context().discovery().sendCustomEvent(msg);

        assertTrue(latch.await(getTestTimeout(), MILLISECONDS));

        receivedMsgs.remove(from.name());

        return receivedMsgs;
    }

    /** */
    protected Map<String, TestDiscoveryMessage> sendOverDiscovery(IgniteEx from, TestMessageType msgType) throws Exception {
        return sendOverDiscovery(from, buildMessage(from, msgType));
    }

    /** */
    protected TestDiscoveryMessage buildMessage(IgniteEx node, TestMessageType msgType) throws Exception {
        return msgType.build(nodeComponentVersions(node), ru(node).features()::isActive);
    }
}
