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

import org.apache.ignite.IgniteSet;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Base class for all IgniteSet single-key requests.
 */
public abstract class ClientIgniteSetKeyRequest extends ClientIgniteSetRequest {
    /** Key. */
    private final Object key;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientIgniteSetKeyRequest(BinaryReaderEx reader) {
        super(reader);

        // Clients can enable deserialized values on server so that user objects are stored the same way
        // as if we were using "thick" API.
        // This is needed when both thick and thin APIs work with the same IgniteSet AND custom user types.
        boolean keepBinary = reader.readBoolean();
        key = keepBinary ? reader.readObjectDetached() : reader.readObject();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteSet<Object> igniteSet = igniteSet(ctx);

        if (igniteSet == null)
            return notFoundResponse();

        return process(igniteSet, key);
    }

    /**
     * Processes the key request.
     *
     * @param set Ignite set.
     * @param key Key.
     * @return Response.
     */
    abstract ClientResponse process(IgniteSet<Object> set, Object key);
}
