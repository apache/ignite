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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;


/**
 * IndexQuery request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientCacheIndexQueryRequest extends ClientCacheRequest {
    /** IndexQuery. */
    private final IndexQuery qry;

    /** Page size. */
    private final int pageSize;

    /**
     * @param reader Reader.
     */
    public ClientCacheIndexQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        pageSize = reader.readInt();

        boolean loc = reader.readBoolean();

        int part = reader.readInt();

        String valType = reader.readString();

        List<IndexQueryCriterion> criteria = reader.readObject();

        String idxName = (String)reader.readObjectDetached();

        Object filterObj = reader.readObjectDetached();

        qry = new IndexQuery(valType, idxName);

        qry.setPageSize(pageSize);
        qry.setLocal(loc);

        if (part >= 0)
            qry.setPartition(part);

        if (criteria != null)
            qry.setCriteria(Arrays.asList(criteria.toArray()));

        if (filterObj != null)
            qry.setFilter(((BinaryObject)filterObj).deserialize());
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = !isKeepBinary() ? rawCache(ctx) : cache(ctx);

        ctx.incrementCursors();

        try {
            QueryCursor cur = cache.query(qry);

            ClientCacheEntryQueryCursor cliCur = new ClientCacheEntryQueryCursor(cur, pageSize, ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheQueryResponse(requestId(), cliCur);
        }
        catch (Exception e) {
            ctx.decrementCursors();

            throw e;
        }
    }
}
