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

package org.apache.ignite.client.handler;

import java.util.BitSet;

import org.apache.ignite.client.proto.ProtocolVersion;
import org.apache.ignite.internal.tostring.S;

/**
 * Client connection context.
 */
class ClientContext {
    /** Version. */
    private final ProtocolVersion version;

    /** Client type code. */
    private final int clientCode;

    /** Feature set. */
    private final BitSet features;

    /**
     * Constructor.
     *
     * @param version Version.
     * @param clientCode Client type code.
     * @param features Feature set.
     */
    ClientContext(ProtocolVersion version, int clientCode, BitSet features) {
        this.version = version;
        this.clientCode = clientCode;
        this.features = features;
    }

    /**
     * Gets the protocol version.
     *
     * @return Protocol version.
     */
    public ProtocolVersion version() {
        return version;
    }

    /**
     * Gets the client code.
     *
     * @return Client code.
     */
    public int clientCode() {
        return clientCode;
    }

    /**
     * Gets the features.
     *
     * @return Features.
     */
    public BitSet features() {
        return features;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientContext.class, this);
    }
}
