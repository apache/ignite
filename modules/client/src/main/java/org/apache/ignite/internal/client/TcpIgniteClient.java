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

package org.apache.ignite.internal.client;

import java.util.function.BiFunction;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Implementation of {@link IgniteClient} over TCP protocol.
 */
public class TcpIgniteClient implements Ignite {
    /** Channel. */
    private final ReliableChannel ch;

    /** Tables. */
    private final ClientTables tables;

    /**
     * Constructor.
     *
     * @param cfg Config.
     */
    public TcpIgniteClient(IgniteClientConfiguration cfg) {
        this(TcpClientChannel::new, cfg);
    }

    /**
     * Constructor with custom channel factory.
     *
     * @param chFactory Channel factory.
     * @param cfg Config.
     */
    public TcpIgniteClient(BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory,
            IgniteClientConfiguration cfg) {
        ch = new ReliableChannel(chFactory, cfg);

        try {
            // TODO: Async init.
            ch.channelsInit();
        }
        catch (Exception e) {
            ch.close();
            throw e;
        }

        tables = new ClientTables(ch);
    }

    /**
     * Initializes new instance of {@link IgniteClient}.
     *
     * @param cfg Thin client configuration.
     * @return Client with successfully opened thin client connection.
     */
    public static Ignite start(IgniteClientConfiguration cfg) throws IgniteClientException {
        return new TcpIgniteClient(cfg);
    }

    /** {@inheritDoc} */
    @Override public IgniteTables tables() {
        return tables;
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ch.close();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        // TODO: improve and finalize IGNITE-15164.
        return null;
    }
}
