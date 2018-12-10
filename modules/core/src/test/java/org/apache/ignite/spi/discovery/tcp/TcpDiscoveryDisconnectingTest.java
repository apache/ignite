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

package org.apache.ignite.spi.discovery.tcp;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryEnsureDelivery;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.mockito.internal.util.reflection.Whitebox;

/**
 *
 */
public class TcpDiscoveryDisconnectingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomMessageWhileSpiIsStopping() throws Exception {
        List<DummyCustomDiscoveryMessage> messages = new CopyOnWriteArrayList<>();

        startGrids(3);

        CustomEventListener<DummyCustomDiscoveryMessage> listener = new CustomEventListener<DummyCustomDiscoveryMessage>() {
            @Override public void onCustomEvent(
                AffinityTopologyVersion topVer,
                ClusterNode snd,
                DummyCustomDiscoveryMessage msg) {
                messages.add(msg);
            }
        };

        G.allGrids().forEach(new Consumer<Ignite>() {
            @Override public void accept(Ignite ignite) {
                ((IgniteEx)ignite)
                    .context()
                    .discovery()
                    .setCustomEventListener(DummyCustomDiscoveryMessage.class, listener);
            }
        });

        grid(0).cluster().active(true);

        TcpDiscoverySpi coordDisco = (TcpDiscoverySpi)grid(0).configuration().getDiscoverySpi();

        TcpDiscoverySpi nodeDisco = (TcpDiscoverySpi)grid(1).configuration().getDiscoverySpi();

        TcpDiscoveryImpl impl = (TcpDiscoveryImpl)Whitebox.getInternalState(nodeDisco, "impl");
        Whitebox.setInternalState(impl, "spiState", TcpDiscoverySpiState.DISCONNECTING);

        // Custom message on a singleton cluster shouldn't break consistency of PendingMessages.
        sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return messages.size() ==  3;
            }
        }, 10000);

        stopAllGrids();
    }

    /**
     * @param disco Discovery SPI.
     * @param id Message id.
     */
    private void sendDummyCustomMessage(TcpDiscoverySpi disco, IgniteUuid id) {
        disco.sendCustomEvent(new CustomMessageWrapper(new DummyCustomDiscoveryMessage(id)));
    }

    /**
     *
     */
    @TcpDiscoveryEnsureDelivery
    private static class DummyCustomDiscoveryMessage implements DiscoveryCustomMessage {
        /** */
        private final IgniteUuid id;

        /**
         * @param id Message id.
         */
        DummyCustomDiscoveryMessage(IgniteUuid id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }
    }
}
