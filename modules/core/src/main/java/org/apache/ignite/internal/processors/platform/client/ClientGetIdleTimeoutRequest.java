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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.ClientConnectorConfiguration;

/**
 * Get idle timeout request.
 */
public class ClientGetIdleTimeoutRequest extends ClientRequest {
    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientGetIdleTimeoutRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        return new ClientLongResponse(requestId(), getEffectiveIdleTimeout(ctx));
    }

    /**
     * Gets the effective idle timeout.
     *
     * @param ctx Context.
     * @return Idle timeout.
     */
    private static long getEffectiveIdleTimeout(ClientConnectionContext ctx) {
        ClientConnectorConfiguration cfg = ctx.kernalContext().config().getClientConnectorConfiguration();

        return cfg == null ? ClientConnectorConfiguration.DFLT_IDLE_TIMEOUT : cfg.getIdleTimeout();
    }
}
