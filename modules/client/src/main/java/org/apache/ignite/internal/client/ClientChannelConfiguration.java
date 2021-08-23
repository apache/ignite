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

import java.net.InetSocketAddress;
import org.apache.ignite.client.IgniteClientConfiguration;

/**
 * Configuration required to initialize {@link TcpClientChannel}.
 */
final class ClientChannelConfiguration {
    /** Host. */
    private final InetSocketAddress addr;

    /** Client config. */
    private final IgniteClientConfiguration cfg;

    /**
     * Constructor.
     *
     * @param cfg Config.
     * @param addr Address.
     */
    ClientChannelConfiguration(IgniteClientConfiguration cfg, InetSocketAddress addr) {
        this.cfg = cfg;
        this.addr = addr;
    }

    /**
     * @return Address.
     */
    public InetSocketAddress getAddress() {
        return addr;
    }

    /**
     * @return Config.
     */
    public IgniteClientConfiguration clientConfiguration() {
        return cfg;
    }
}
