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

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Continuous query response.
 */
class ClientCacheQueryContinuousResponse extends ClientResponse {
    /** */
    private final ClientCacheQueryContinuousHandle handle;

    /** */
    private final long continuousQueryId;

    /** */
    private final Collection<String> columnNames;

    /**
     * Ctor.
     * @param reqId Request id.
     * @param handle Handle.
     * @param continuousQueryId Continuous query handle id.
     */
    public ClientCacheQueryContinuousResponse(long reqId, ClientCacheQueryContinuousHandle handle,
                                              long continuousQueryId, @Nullable Collection<String> columnNames) {
        super(reqId);
        this.handle = handle;

        this.continuousQueryId = continuousQueryId;

        //noinspection AssignmentOrReturnOfFieldWithMutableType
        this.columnNames = columnNames;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeLong(continuousQueryId);

        if (columnNames != null) {
            writer.writeInt(columnNames.size());

            for (String col : columnNames)
                writer.writeString(col);
        }
    }

    /** {@inheritDoc} */
    @Override public void onSent() {
        super.onSent();

        handle.startNotifications(continuousQueryId);
    }
}
