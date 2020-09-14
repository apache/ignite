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

package org.apache.ignite.internal.client.thin;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Class test ReliableChannel channels re-initialization.
 */
public class ReliableChannelTest {
    /** Mock factory for creating new channels. */
    private final Function<ClientChannelConfiguration, ClientChannel> chFactory = cfg -> new TestClientChannel();

    /** Checks that channel holders are not reinited for static address configuration. */
    @Test
    public void testChannelsNotReinitForStaticAddressConfiguration() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002");

        checkDoesNotReinit(ccfg);
    }

    /** Checks that channel holders are not reinited if address finder return the same list of addresses. */
    @Test
    public void testChannelsNotReinitForStableDynamicAddressConfiguration() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddressesFinder(new TestAddressFinder("127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"));

        checkDoesNotReinit(ccfg);
    }

    /** */
    private void checkDoesNotReinit(ClientConfiguration ccfg) {
        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.initConnection();
        List<ReliableChannel.ClientChannelHolder> originalChannels = rc.getChannelHolders();

        // Imitate topology change.
        rc.initChannelHolders(true);
        List<ReliableChannel.ClientChannelHolder> newChannels = rc.getChannelHolders();

        assertSame(originalChannels, newChannels);
        IntStream.range(0, 3).forEach(i -> {
            assertSame(originalChannels.get(i), newChannels.get(i));
            assertFalse(originalChannels.get(i).isClosed());
        });
        assertEquals(3, newChannels.size());
    }

    /** Checks that node channels are persisted if channels are reinit with static address configuration. */
    @Test
    public void testNodeChannelsAreNotCleaned() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002");

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.initConnection();
        // Trigger TestClientChannel creation.
        rc.service(null, null, null);

        assertEquals(1, rc.nodeChannels.size());

        // Imitate topology change.
        rc.initChannelHolders(true);

        assertEquals(1, rc.nodeChannels.size());
    }

    /** Checks that channels are changed (add new, remove old) and close channels if reinitialization performed. */
    @Test
    public void testDynamicAddressReinitializedCorrectly() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddressesFinder(new TestAddressFinder("127.0.0.1:8000", "127.0.0.1:8003"));

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.initConnection();

        List<ReliableChannel.ClientChannelHolder> originChannels = Collections.unmodifiableList(rc.getChannelHolders());
        // Imitate topology change.
        rc.initChannelHolders(true);

        List<ReliableChannel.ClientChannelHolder> closedChannels = originChannels.stream()
            .filter(r -> r.isClosed())
            .collect(Collectors.toList());

        assertEquals(2, closedChannels.size());

        List<ReliableChannel.ClientChannelHolder> reuseChannel = originChannels.stream()
            .filter(c -> !c.isClosed())
            .collect(Collectors.toList());

        assertEquals(1, reuseChannel.size());

        List<ReliableChannel.ClientChannelHolder> newChannels = rc.getChannelHolders();
        assertEquals(2, newChannels.size());
        assertTrue(newChannels.get(0) == reuseChannel.get(0) || newChannels.get(1) == reuseChannel.get(0));
        newChannels.forEach(c -> assertFalse(c.isClosed()));
    }

    /** Check that node channels are cleaned in case of full reinitialization. */
    @Test
    public void testThatNodeChannelsCleanFullReinitialization() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddressesFinder(new TestAddressFinder("127.0.0.1:8003", "127.0.0.1:8004"));

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.initConnection();
        // Trigger TestClientChannel creation.
        rc.service(null, null, null);

        assertEquals(1, rc.nodeChannels.size());

        // Imitate topology change.
        rc.initChannelHolders(true);

        assertEquals(0, rc.nodeChannels.size());
    }

    /** Mock for client channel. */
    private static class TestClientChannel implements ClientChannel {
        /** */
        private final UUID serverNodeId = UUID.randomUUID();

        /** {@inheritDoc} */
        @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader) throws ClientException, ClientAuthorizationException, ClientServerError, ClientConnectionException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ProtocolContext protocolCtx() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public UUID serverNodeId() {
            return serverNodeId;
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion serverTopologyVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void addTopologyChangeListener(Consumer<ClientChannel> lsnr) {

        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(NotificationListener lsnr) {

        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {

        }
    }

    /**
     * Mock for address finder. By first request return fixed list of addresses.
     * Response on second request set with constructor.
     */
    private static class TestAddressFinder implements ClientAddressFinder {

        /** Number of request to the finder. */
        private int reqNum = -1;

        /** List of addresses that returns on second request. */
        private final String[] newAddrs;

        /** */
        public TestAddressFinder(String... newAddrs) {
            this.newAddrs = newAddrs;
        }

        /** {@inheritDoc} */
        @Override public String[] getServerAddresses() {
            reqNum += 1;

            if (reqNum == 0)
                return new String[]{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"};

            if (reqNum == 1)
                return newAddrs.clone();

            throw new IllegalStateException("Must not be there");
        }
    }
}
