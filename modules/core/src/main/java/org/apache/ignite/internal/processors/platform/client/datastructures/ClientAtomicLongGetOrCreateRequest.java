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

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientNullableLongResponse;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.jetbrains.annotations.Nullable;

/**
 * Gets or creates atomic long by name.
 */
public class ClientAtomicLongGetOrCreateRequest extends ClientRequest {
    private final String name;

    private final boolean create;

    private final long initVal;

    private final AtomicConfiguration atomicConfiguration;

    public ClientAtomicLongGetOrCreateRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        create = reader.readBoolean();

        if (create) {
            initVal = reader.readLong();
            atomicConfiguration = readAtomicConfiguration(reader);
        } else {
            initVal = 0;
            atomicConfiguration = null;
        }
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            IgniteAtomicLong atomicLong = ctx.kernalContext().dataStructures().atomicLong(name, atomicConfiguration, initVal, create);

            Long resId = atomicLong == null ? null : ctx.resources().put(atomicLong);

            return new ClientNullableLongResponse(requestId(), resId);
        } catch (IgniteCheckedException e) {
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
