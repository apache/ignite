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

package org.apache.ignite.internal.processors.platform.client.datastructures;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.jetbrains.annotations.Nullable;

/**
 * Gets or creates atomic long by name.
 */
public class ClientAtomicLongCreateRequest extends ClientRequest {
    /** Atomic long name. */
    private final String name;

    /** Initial value. */
    private final long initVal;

    /** Configuration. */
    private final AtomicConfiguration atomicConfiguration;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientAtomicLongCreateRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        initVal = reader.readLong();
        atomicConfiguration = readAtomicConfiguration(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            ctx.kernalContext().dataStructures().atomicLong(name, atomicConfiguration, initVal, true);

            return new ClientResponse(requestId());
        }
        catch (IgniteCheckedException e) {
            return new ClientResponse(requestId(), e.getMessage());
        }
    }

    /**
     * Reads the atomic configuration.
     *
     * @param reader Reader.
     * @return Config.
     */
    @Nullable private static AtomicConfiguration readAtomicConfiguration(BinaryRawReader reader) {
        if (!reader.readBoolean())
            return null;

        return new AtomicConfiguration()
                .setAtomicSequenceReserveSize(reader.readInt())
                .setCacheMode(CacheMode.fromOrdinal(reader.readByte()))
                .setBackups(reader.readInt())
                .setGroupName(reader.readString());
    }
}
