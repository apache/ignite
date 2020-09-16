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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
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

    /** */
    private final String[] dfltAddrs = new String[]{"127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802"};

    /** Checks that channel holders are not reinited for static address configuration. */
    @Test
    public void testChannelsNotReinitForStaticAddressConfiguration() {
        ClientConfiguration ccfg = new ClientConfiguration().setAddresses(dfltAddrs);

        checkDoesNotReinit(ccfg);
    }

    /** Checks that channel holders are not reinited if address finder return the same list of addresses. */
    @Test
    public void testChannelsNotReinitForStableDynamicAddressConfiguration() {
        TestAddressFinder finder = new TestAddressFinder()
            .nextAddresesResponse(dfltAddrs)
            .nextAddresesResponse("127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802");

        ClientConfiguration ccfg = new ClientConfiguration().setAddressesFinder(finder);

        checkDoesNotReinit(ccfg);
    }

    /** */
    private void checkDoesNotReinit(ClientConfiguration ccfg) {
        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.channelsInit(false);
        List<ReliableChannel.ClientChannelHolder> originalChannels = rc.getChannelHolders();

        // Imitate topology change.
        rc.initChannelHolders(true);
        List<ReliableChannel.ClientChannelHolder> newChannels = rc.getChannelHolders();

        assertSame(originalChannels, newChannels);
        for (int i = 0; i < 3; ++i) {
            assertSame(originalChannels.get(i), newChannels.get(i));
            assertFalse(originalChannels.get(i).isClosed());
        }
        assertEquals(3, newChannels.size());
    }

    /** Checks that node channels are persisted if channels are reinit with static address configuration. */
    @Test
    public void testNodeChannelsAreNotCleaned() {
        ClientConfiguration ccfg = new ClientConfiguration().setAddresses(dfltAddrs);

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.channelsInit(false);
        // Trigger TestClientChannel creation.
        rc.service(null, null, null);

        assertEquals(1, rc.getNodeChannels().size());

        // Imitate topology change.
        rc.initChannelHolders(true);

        assertEquals(1, rc.getNodeChannels().size());
    }

    /** Checks that channels are changed (add new, remove old) and close channels if reinitialization performed. */
    @Test
    public void testDynamicAddressReinitializedCorrectly() {
        TestAddressFinder finder = new TestAddressFinder()
            .nextAddresesResponse(dfltAddrs)
            .nextAddresesResponse("127.0.0.1:10800", "127.0.0.1:10803");

        ClientConfiguration ccfg = new ClientConfiguration().setAddressesFinder(finder);

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.channelsInit(false);

        List<ReliableChannel.ClientChannelHolder> originChannels = Collections.unmodifiableList(rc.getChannelHolders());
        // Imitate topology change.
        rc.initChannelHolders(true);

        assertEquals(2, F.size(originChannels, ReliableChannel.ClientChannelHolder::isClosed));

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
        TestAddressFinder finder = new TestAddressFinder()
            .nextAddresesResponse(dfltAddrs)
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10804");

        ClientConfiguration ccfg = new ClientConfiguration().setAddressesFinder(finder);

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);
        rc.channelsInit(false);
        // Trigger TestClientChannel creation.
        rc.service(null, null, null);

        assertEquals(1, rc.getNodeChannels().size());

        // Imitate topology change.
        rc.initChannelHolders(true);

        assertEquals(0, rc.getNodeChannels().size());
    }

    /** Should fail if default channel is not initialized. */
    @Test(expected = TestChannelException.class)
    public void testFailOnInitIfDefaultChannelFailed() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddresses(dfltAddrs)
            .setPartitionAwarenessEnabled(true);

        ReliableChannel rc = new ReliableChannel(cfg -> new TestFailureClientChannel(), ccfg, null);
        rc.channelsInit(false);
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

    /** Mock client channel that fails on initialization. */
    private static class TestFailureClientChannel extends TestClientChannel {
        /** Constructor that fails. */
        private TestFailureClientChannel() {
            throw new TestChannelException();
        }
    }

    /** TestFailureClientChannel failed with this Exception. */
    private static class TestChannelException extends RuntimeException {}

    /**
     * Mock for address finder.
     */
    private static class TestAddressFinder implements ClientAddressFinder {

        /** Queue of list addresses. Every new request poll this queue. */
        private final Queue<String[]> addrResQueue;

        /** */
        private TestAddressFinder() {
            addrResQueue = new LinkedList<>();
        }

        /** Configure result for every next {@link #getServerAddresses()} request. */
        private TestAddressFinder nextAddresesResponse(String... addrs) {
            addrResQueue.add(addrs);

            return this;
        }

        /** {@inheritDoc} */
        @Override public String[] getServerAddresses() {
            if (addrResQueue.isEmpty())
                throw new IllegalStateException("Server address request is not expected.");

            return addrResQueue.poll();
        }
    }
}
