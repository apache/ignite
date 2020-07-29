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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.Collection;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache names response.
 */
public class ClientCacheGetNamesResponse extends ClientResponse {
    /** Cache names. */
    private final Collection<String> cacheNames;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param cacheNames Cache names.
     */
    ClientCacheGetNamesResponse(long reqId, Collection<String> cacheNames) {
        super(reqId);

        assert cacheNames != null;

        this.cacheNames = cacheNames;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(cacheNames.size());

        for (String name : cacheNames) {
            writer.writeString(name);
        }
    }
}
