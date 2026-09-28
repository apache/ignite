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
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.MessagesPluginProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.resolveTestMessageClasses;

/** */
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

        to.context().io().addMessageListener(topic, (nodeId, rcvd, plc) -> {
            got.set((T)rcvd);

            latch.countDown();
        });

        ClusterNode rcvNode = from.context().discovery().node(to.localNode().id());

        from.context().io().sendToCustomTopic(rcvNode, topic, msg, PUBLIC_POOL);

        assertTrue(latch.await(getTestTimeout(), MILLISECONDS));

        return got.get();
    }

    /** */
    protected TestDiscoveryMessage send(IgniteEx from, IgniteEx to, TestMessageType msgType) throws Exception {
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
