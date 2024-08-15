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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.cache.query.InIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.ARR_LIST;

/**
 * IndexQuery request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientCacheIndexQueryRequest extends ClientCacheQueryRequest {
    /** IndexQuery. */
    private final IndexQuery qry;

    /** Page size. */
    private final int pageSize;

    /**
     * @param reader Reader.
     * @param protocolCtx
     */
    public ClientCacheIndexQueryRequest(
        BinaryRawReaderEx reader,
        ClientProtocolContext protocolCtx
    ) {
        super(reader);

        pageSize = reader.readInt();

        boolean loc = reader.readBoolean();

        int part = reader.readInt();

        int limit = 0;
        if (protocolCtx.isFeatureSupported(ClientBitmaskFeature.INDEX_QUERY_LIMIT))
            limit = reader.readInt();

        String valType = reader.readString();

        String idxName = reader.readString();

        byte arrMark = reader.readByte();

        List<IndexQueryCriterion> criteria = null;

        if (arrMark == ARR_LIST) {
            int critSize = reader.readInt();

            criteria = new ArrayList<>(critSize);

            for (int i = 0; i < critSize; i++)
                criteria.add(readCriterion(reader));
        }

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

        if (limit > 0)
            qry.setLimit(limit);
    }

    /** */
    private IndexQueryCriterion readCriterion(BinaryRawReaderEx reader) {
        byte type = reader.readByte();

        if (type == (byte)0)
            return readRangeCriterion(reader);
        else if (type == (byte)1)
            return readInCriterion(reader);

        throw new IgniteException("Unknown IndexQuery criterion type: " + type);
    }

    /** */
    private IndexQueryCriterion readRangeCriterion(BinaryRawReaderEx reader) {
        String field = reader.readString();

        boolean lowerIncl = reader.readBoolean();
        boolean upperIncl = reader.readBoolean();
        boolean lowerNull = reader.readBoolean();
        boolean upperNull = reader.readBoolean();

        Object lower = reader.readObjectDetached();
        Object upper = reader.readObjectDetached();

        RangeIndexQueryCriterion r = new RangeIndexQueryCriterion(field, lower, upper);
        r.lowerIncl(lowerIncl);
        r.upperIncl(upperIncl);
        r.lowerNull(lowerNull);
        r.upperNull(upperNull);

        return r;
    }

    /** */
    private IndexQueryCriterion readInCriterion(BinaryRawReaderEx reader) {
        String field = reader.readString();

        int valsCnt = reader.readInt();

        List<Object> vals = new ArrayList<>(valsCnt);

        for (int i = 0; i < valsCnt; i++)
            vals.add(reader.readObject());

        return new InIndexQueryCriterion(field, vals);
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache<Object, Object> cache = qry.getFilter() != null && !isKeepBinary() ?
            rawCache(ctx) : cache(ctx);

        if (qry.getPartition() != null)
            updateAffinityMetrics(ctx, qry.getPartition());

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
