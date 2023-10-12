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

import java.util.UUID;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientObjectResponse;
import org.jetbrains.annotations.Nullable;

public class ClientServiceCallResponse extends ClientObjectResponse {
    /** Current topology version of the service. */
    private final long topVer;

    /** Service node ids. */
    private final @Nullable UUID[] nodeIds;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param res Service call result.
     * @param topVer Current topology version of the service.
     * @param nodeIds Service node ids.
     */
    public ClientServiceCallResponse(long reqId, @Nullable Object res, long topVer, @Nullable UUID[] nodeIds) {
        super(reqId, res);

        this.topVer = topVer;
        this.nodeIds = nodeIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeLong(topVer);

        if (nodeIds == null)
            writer.writeInt(0);
        else {
            writer.writeInt(nodeIds.length);

            for (UUID node : nodeIds) {
                writer.writeLong(node.getMostSignificantBits());
                writer.writeLong(node.getLeastSignificantBits());
            }
        }
    }
}
