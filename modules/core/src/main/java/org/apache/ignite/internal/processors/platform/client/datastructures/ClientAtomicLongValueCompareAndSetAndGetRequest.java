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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientLongResponse;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Atomic long compare and set and get request.
 */
public class ClientAtomicLongValueCompareAndSetAndGetRequest extends ClientAtomicLongRequest {
    /** */
    private final long expected;

    /** */
    private final long val;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientAtomicLongValueCompareAndSetAndGetRequest(BinaryRawReader reader) {
        super(reader);

        expected = reader.readLong();
        val = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        GridCacheAtomicLongImpl atomicLong = atomicLong(ctx);

        if (atomicLong == null)
            return notFoundResponse();

        return new ClientLongResponse(requestId(), atomicLong.compareAndSetAndGet(expected, val));
    }
}
