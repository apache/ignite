/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.thin;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Adds failover abd thread-safety to {@link ClientChannel}.
 */
final class ReliableChannel implements AutoCloseable {
    /** Raw channel. */
    private final Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory;

    /** Servers count. */
    private final int srvCnt;

    /** Primary server. */
    private InetSocketAddress primary;

    /** Backup servers. */
    private final Deque<InetSocketAddress> backups = new LinkedList<>();

    /** Channel. */
    private ClientChannel ch;

    /** Ignite config. */
    private final ClientConfiguration clientCfg;

    /** Channel is closed. */
    private boolean closed;

    /**
     * Constructor.
     */
    ReliableChannel(
        Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory,
        ClientConfiguration clientCfg
    ) throws ClientException {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.chFactory = chFactory;
        this.clientCfg = clientCfg;

        List<InetSocketAddress> addrs = parseAddresses(clientCfg.getAddresses());

        srvCnt = addrs.size();

        primary = addrs.get(new Random().nextInt(addrs.size())); // we already verified there is at least one address

        for (InetSocketAddress a : addrs) {
            if (a != primary)
                backups.add(a);
        }

        ClientConnectionException lastEx = null;

        for (int i = 0; i < addrs.size(); i++) {
            try {
                ch = chFactory.apply(new ClientChannelConfiguration(clientCfg).setAddress(primary)).get();

                return;
            } catch (ClientConnectionException e) {
                lastEx = e;

                rollAddress();
            }
        }

        throw lastEx;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws Exception {
        closed = true;

        if (ch != null) {
            ch.close();

            ch = null;
        }
    }

    /**
     * Send request and handle response.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        ClientConnectionException failure = null;

        for (int i = 0; i < srvCnt; i++) {
            ClientChannel ch = null;

            try {
                ch = channel();

                return ch.service(op, payloadWriter, payloadReader);
            }
            catch (ClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                changeServer(ch);
            }
        }

        throw failure;
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<PayloadInputChannel, T> payloadReader)
        throws ClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter) throws ClientException {
        service(op, payloadWriter, null);
    }

    /**
     * @return host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private static List<InetSocketAddress> parseAddresses(String[] addrs) throws ClientException {
        if (F.isEmpty(addrs))
            throw new ClientException("Empty addresses");

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            try {
                ranges.add(HostAndPortRange.parse(
                    a,
                    ClientConnectorConfiguration.DFLT_PORT,
                    ClientConnectorConfiguration.DFLT_PORT + ClientConnectorConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
                ));
            }
            catch (IgniteCheckedException e) {
                throw new ClientException(e);
            }
        }

        return ranges.stream()
            .flatMap(r -> IntStream
                .rangeClosed(r.portFrom(), r.portTo()).boxed()
                .map(p -> new InetSocketAddress(r.host(), p))
            )
            .collect(Collectors.toList());
    }

    /** */
    private synchronized ClientChannel channel() {
        if (closed)
            throw new ClientException("Channel is closed");

        if (ch == null) {
            try {
                ch = chFactory.apply(new ClientChannelConfiguration(clientCfg).setAddress(primary)).get();
            }
            catch (ClientConnectionException e) {
                rollAddress();

                throw e;
            }
        }

        return ch;
    }

    /** */
    private void rollAddress() {
        if (!backups.isEmpty()) {
            backups.addLast(primary);

            primary = backups.removeFirst();
        }
    }

    /** */
    private synchronized void changeServer(ClientChannel oldCh) {
        if (oldCh == ch && ch != null) {
            rollAddress();

            U.closeQuiet(ch);

            ch = null;
        }
    }
}
