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

package org.apache.ignite.internal.processors.platform.client.service;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.client.thin.ClientUtils;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.jetbrains.annotations.Nullable;

/** Service topology response. */
public class ClientServiceMappingsResponse extends ClientResponse {
    /** Service instance nodes. */
    @Nullable private final Collection<UUID> svcsNodes;

    /**
     * @param reqId Request id.
     * @param svcsNodes Services instance nodes.
     */
    public ClientServiceMappingsResponse(long reqId, @Nullable Collection<UUID> svcsNodes) {
        super(reqId);

        this.svcsNodes = svcsNodes;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryWriterEx writer) {
        super.encode(ctx, writer);

        ClientUtils.collection(
            svcsNodes,
            writer.out(),
            (out, uuid) -> {
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());
            });
    }
}
