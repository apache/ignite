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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Class test ReliableChannel channels re-initialization.
 */
public class ReliableChannelTest {
    /** Mock factory for creating new channels. */
    private final BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory =
            (cfg, hnd) -> new TestClientChannel();

    /** */
    private final String[] dfltAddrs = new String[]{"127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802"};

    /**
     * Checks that it is possible configure addresses with duplication (for load balancing).
     */
    @Test
    public void testDuplicatedAddressesAreValid() {
        ClientConfiguration ccfg = new ClientConfiguration().setAddresses(
            "127.0.0.1:10800", "127.0.0.1:10800", "127.0.0.1:10801");

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

        rc.channelsInit();

        assertEquals(3, rc.getChannelHolders().size());
    }

    /**
     * Checks that in case if address specified without port, the default port will be processed first
     */
    @Test
    public void testAddressWithoutPort() {
        ClientConfiguration ccfg = new ClientConfiguration().setAddresses("127.0.0.1");

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

        rc.channelsInit();

        assertEquals(ClientConnectorConfiguration.DFLT_PORT_RANGE + 1, rc.getChannelHolders().size());

        assertEquals(ClientConnectorConfiguration.DFLT_PORT, F.first(rc.getChannelHolders()).getAddress().getPort());

        assertEquals(0, rc.getCurrentChannelIndex());
    }

    /**
     * Checks that ReliableChannel chooses random address as default from the set of addresses with the same (minimal) port.
     */
    @Test
    public void testDefaultChannelBalancing() {
        assertEquals(new HashSet<>(F.asList("127.0.0.2:10800", "127.0.0.3:10800", "127.0.0.4:10800")),
            usedDefaultChannels("127.0.0.1:10801..10809", "127.0.0.2", "127.0.0.3:10800", "127.0.0.4:10800..10809"));

        assertEquals(new HashSet<>(F.asList("127.0.0.1:10800", "127.0.0.2:10800", "127.0.0.3:10800", "127.0.0.4:10800")),
            usedDefaultChannels("127.0.0.1:10800", "127.0.0.2:10800", "127.0.0.3:10800", "127.0.0.4:10800"));
    }

    /** */
    private Set<String> usedDefaultChannels(String... addrs) {
        ClientConfiguration ccfg = new ClientConfiguration().setAddresses(addrs);

        Set<String> usedChannels = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

            rc.channelsInit();

            usedChannels.add(rc.getChannelHolders().get(rc.getCurrentChannelIndex()).getAddress().toString());
        }

        return usedChannels;
    }

    /**
     * Checks that reinitialization of duplicated address is correct.
     */
    @Test
    public void testReinitDuplicatedAddress() {
        TestAddressFinder finder = new TestAddressFinder()
            .nextAddresesResponse("127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802")
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10804", "127.0.0.1:10805")
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10804", "127.0.0.1:10806")
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10803", "127.0.0.1:10806")
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10803", "127.0.0.1:10803")
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10803", "127.0.0.1:10804")
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10804", "127.0.0.1:10804")
            .nextAddresesResponse("127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802");

        ClientConfiguration ccfg = new ClientConfiguration().setAddressesFinder(finder);
        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

        Supplier<List<String>> holderAddresses = () -> rc.getChannelHolders().stream()
            .map(h -> h.getAddress().toString())
            .sorted()
            .collect(Collectors.toList());

        Consumer<List<String>> assertAddrReInitAndEqualsTo = (addrs) -> {
            rc.channelsInit();

            assertEquals(addrs, holderAddresses.get());
        };

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10803", "127.0.0.1:10804", "127.0.0.1:10805"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10803", "127.0.0.1:10804", "127.0.0.1:10806"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10803", "127.0.0.1:10803", "127.0.0.1:10806"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10803", "127.0.0.1:10803", "127.0.0.1:10803"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10803", "127.0.0.1:10803", "127.0.0.1:10804"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10803", "127.0.0.1:10804", "127.0.0.1:10804"));

        assertAddrReInitAndEqualsTo.accept(Arrays.asList("127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802"));
    }

    /**
     * Checks that channel holders are not reinited for static address configuration.
     */
    @Test
    public void testChannelsNotReinitForStaticAddressConfiguration() {
        ClientConfiguration ccfg = new ClientConfiguration().setAddresses(dfltAddrs);

        checkDoesNotReinit(ccfg);
    }

    /**
     * Checks that channel holders are not reinited if address finder return the same list of addresses.
     */
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

        rc.channelsInit();

        List<ReliableChannel.ClientChannelHolder> originalChannels = rc.getChannelHolders();
        List<ReliableChannel.ClientChannelHolder> copyOriginalChannels = new ArrayList<>(originalChannels);

        // Imitate topology change.
        rc.initChannelHolders();

        List<ReliableChannel.ClientChannelHolder> newChannels = rc.getChannelHolders();

        assertSame(originalChannels, newChannels);

        for (int i = 0; i < 3; ++i) {
            assertSame(copyOriginalChannels.get(i), newChannels.get(i));
            assertFalse(copyOriginalChannels.get(i).isClosed());
        }

        assertEquals(3, newChannels.size());
    }

    /**
     * Checks that node channels are persisted if channels are reinit with static address configuration.
     */
    @Test
    public void testNodeChannelsAreNotCleaned() {
        ClientConfiguration ccfg = new ClientConfiguration()
                .setAddresses(dfltAddrs)
                .setPartitionAwarenessEnabled(false);

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

        rc.channelsInit();

        // Trigger TestClientChannel creation.
        rc.service(null, null, null);

        assertEquals(1, rc.getNodeChannels().size());

        // Imitate topology change.
        rc.initChannelHolders();

        assertEquals(1, rc.getNodeChannels().size());
    }

    /**
     * Checks that channels are changed (add new, remove old) and close channels if reinitialization performed.
     */
    @Test
    public void testDynamicAddressReinitializedCorrectly() {
        TestAddressFinder finder = new TestAddressFinder()
            .nextAddresesResponse(dfltAddrs)
            .nextAddresesResponse("127.0.0.1:10800", "127.0.0.1:10803");

        ClientConfiguration ccfg = new ClientConfiguration().setAddressesFinder(finder);

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

        rc.channelsInit();

        List<ReliableChannel.ClientChannelHolder> originChannels = Collections.unmodifiableList(rc.getChannelHolders());

        // Imitate topology change.
        rc.initChannelHolders();

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

    /**
     * Check that node channels are cleaned in case of full reinitialization.
     */
    @Test
    public void testThatNodeChannelsCleanFullReinitialization() {
        TestAddressFinder finder = new TestAddressFinder()
            .nextAddresesResponse(dfltAddrs)
            .nextAddresesResponse("127.0.0.1:10803", "127.0.0.1:10804");

        ClientConfiguration ccfg = new ClientConfiguration()
                .setAddressesFinder(finder)
                .setPartitionAwarenessEnabled(false);

        ReliableChannel rc = new ReliableChannel(chFactory, ccfg, null);

        rc.channelsInit();

        // Trigger TestClientChannel creation.
        rc.service(null, null, null);

        assertEquals(1, rc.getNodeChannels().size());

        // Imitate topology change.
        rc.initChannelHolders();

        assertEquals(0, rc.getNodeChannels().size());
    }

    /**
     * Should fail if default channel is not initialized.
     */
    @Test(expected = TestChannelException.class)
    public void testFailOnInitIfDefaultChannelFailed() {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddresses(dfltAddrs)
            .setPartitionAwarenessEnabled(true);

        ReliableChannel rc = new ReliableChannel((cfg, hnd) -> new TestFailureClientChannel(), ccfg, null);

        rc.channelsInit();
    }

    /**
     * Async operation should fail if cluster is down after send operation.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFailOnAsyncAfterSendOperation() {
        checkFailAfterSendOperation(cache -> {
            try {
                cache.getAsync(0).get();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, false);
    }

    /**
     * Async operation should fail if cluster is down after send operation and handle topology change.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFailOnAsyncTopologyChangeAfterSendOperation() {
        checkFailAfterSendOperation(cache -> {
            try {
                cache.getAsync(0).get();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, true);
    }

    /** */
    private void checkFailAfterSendOperation(Consumer<TcpClientCache> op, boolean channelsReinitOnFail) {
        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddresses(dfltAddrs);

        // Emulate cluster is down after TcpClientChannel#send operation.
        AtomicInteger step = new AtomicInteger();

        ReliableChannel rc = new ReliableChannel((cfg, hnd) -> {
            if (step.getAndIncrement() == 0)
                return new TestAsyncServiceFailureClientChannel();
            else
                return new TestFailureClientChannel();
        }, ccfg, null);

        rc.channelsInit();

        rc.getScheduledChannelsReinit().set(channelsReinitOnFail);

        ClientBinaryMarshaller marsh = mock(ClientBinaryMarshaller.class);
        TcpClientTransactions transactions = mock(TcpClientTransactions.class);

        TcpClientCache cache = new TcpClientCache("", rc, marsh, transactions, null, false, null);

        GridTestUtils.assertThrowsWithCause(() -> op.accept(cache), TestChannelException.class);
    }

    /**
     * Mock for client channel.
     */
    private static class TestClientChannel implements ClientChannel {
        /** */
        private final UUID serverNodeId = UUID.randomUUID();

        /** {@inheritDoc} */
        @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader)
            throws ClientException, ClientAuthorizationException, ClientServerError, ClientConnectionException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T> CompletableFuture<T> serviceAsync(
            ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader) {
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
            /* No-op */
        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(ClientNotificationType type, Long rsrcId,
            NotificationListener lsnr) {
            /* No-op */
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ClientNotificationType type, Long rsrcId) {
            /* No-op */
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            /* No-op */
        }
    }

    /**
     * Mock client channel that fails on initialization.
     */
    private static class TestFailureClientChannel extends TestClientChannel {
        /** Constructor that fails. */
        private TestFailureClientChannel() {
            throw new TestChannelException();
        }
    }

    /**
     * Mock client channel that fails on initialization.
     */
    private static class TestAsyncServiceFailureClientChannel extends TestClientChannel {
        /** {@inheritDoc} */
        @Override public <T> CompletableFuture<T> serviceAsync(ClientOperation op,
                                                               Consumer<PayloadOutputChannel> payloadWriter,
                                                               Function<PayloadInputChannel, T> payloadReader) {
            // Emulate that TcpClientChannel#send is ok, but TcpClientChannel#recieve is failed.
            CompletableFuture<T> fut = new CompletableFuture<>();

            fut.completeExceptionally(new ClientConnectionException(null));

            return fut;
        }
    }

    /**
     * TestFailureClientChannel failed with this Exception.
     */
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

        /**
         * Configure result for every next {@link #getAddresses()} request.
         */
        private TestAddressFinder nextAddresesResponse(String... addrs) {
            addrResQueue.add(addrs);

            return this;
        }

        /** {@inheritDoc} */
        @Override public String[] getAddresses() {
            if (addrResQueue.isEmpty())
                throw new IllegalStateException("Server address request is not expected.");

            return addrResQueue.poll();
        }
    }
}
