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

package org.apache.ignite.internal.client.io;

import java.net.InetSocketAddress;

import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;

/**
 * Client connection multiplexer: manages multiple connections with a shared resource pool (worker threads, etc).
 */
 public interface ClientConnectionMultiplexer {
    /**
     * Initializes this instance.
     *
     * @param clientCfg Client config.
     */
    void start(IgniteClientConfiguration clientCfg);

    /**
     * Stops this instance.
     */
    void stop();

    /**
     * Opens a new connection.
     *
     * @param addr Address.
     * @param msgHnd Incoming message handler.
     * @param stateHnd Connection state handler.
     * @return Created connection.
     * @throws IgniteClientConnectionException when connection can't be established.
     */
    ClientConnection open(
            InetSocketAddress addr,
            ClientMessageHandler msgHnd,
            ClientConnectionStateHandler stateHnd)
            throws IgniteClientConnectionException;
}
