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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Continuous query request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientCacheQueryContinuousRequest extends ClientCacheRequest {
    /** Query. */
    private final ContinuousQuery qry;

    /** */
    private final byte filterPlatform;

    /** */
    private final byte transformerPlatform;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheQueryContinuousRequest(BinaryRawReaderEx reader) {
        super(reader);

        int pageSize = reader.readInt();
        long timeInterval = reader.readLong();
        boolean includeExpired = reader.readBoolean();
        Object filter = reader.readObjectDetached();
        filterPlatform = filter == null ? 0 : reader.readByte();
        Object transformer = reader.readObjectDetached();
        transformerPlatform = transformer == null ? 0 : reader.readByte();
        byte initialQueryType = reader.readByte();

        assert initialQueryType == 0; // TODO: 1 = SQL, 2 = SCAN

        qry = new ContinuousQuery()
                .setPageSize(pageSize)
                .setTimeInterval(timeInterval);

        qry.setIncludeExpired(includeExpired);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ctx.incrementCursors();

        try {
            IgniteCache cache = filterPlatform == ClientPlatform.JAVA && !isKeepBinary() ? rawCache(ctx) : cache(ctx);

            ClientCacheQueryContinuousHandle handle = new ClientCacheQueryContinuousHandle(ctx);
            qry.setLocalListener(handle);

            QueryCursor cursor = cache.query(qry);
            long cursorId = ctx.resources().put(cursor);

            handle.setCursor(cursor, cursorId);

            // TODO: Initial query.
            return new ClientCacheQueryContinuousResponse(requestId(), cursorId, null);
        }
        catch (Exception e) {
            ctx.decrementCursors();
            throw e;
        }
    }
}
