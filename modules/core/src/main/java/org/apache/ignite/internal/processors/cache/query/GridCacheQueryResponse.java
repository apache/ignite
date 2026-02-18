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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMeta;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Page of cache query response.
 */
public class GridCacheQueryResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    @Order(4)
    boolean finished;

    /** */
    @Order(5)
    long reqId;

    /** */
    @Order(value = 6, method = "errorMessage")
    @Nullable ErrorMessage errMsg;

    /** */
    @Order(7)
    boolean fields;

    /** */
    @Order(8)
    IndexQueryResultMeta idxQryMetadata;

    /** */
    @Order(9)
    Collection<byte[]> dataBytes;

    /** */
    private Collection<Object> data;

    /**
     * Empty constructor.
     */
    public GridCacheQueryResponse() {
        //No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param finished Last response or not.
     * @param fields Fields query or not.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, boolean finished, boolean fields, boolean addDepInfo) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.finished = finished;
        this.fields = fields;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param err Error.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, Throwable err, boolean addDepInfo) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        errMsg = new ErrorMessage(err);
        this.addDepInfo = addDepInfo;

        finished = true;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (dataBytes == null && data != null)
            dataBytes = marshalCollection(data, cctx);

        if (addDepInfo && !F.isEmpty(data)) {
            for (Object o : data) {
                if (o instanceof Map.Entry) {
                    Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;

                    prepareObject(e.getKey(), cctx);
                    prepareObject(e.getValue(), cctx);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (data == null)
            data = unmarshalCollection0(dataBytes, ctx, ldr);
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected <T> List<T> unmarshalCollection0(@Nullable Collection<byte[]> byteCol,
        GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        List<T> col = new ArrayList<>(byteCol.size());

        Marshaller marsh = ctx.marshaller();

        ClassLoader ldr0 = U.resolveClassLoader(ldr, ctx.gridConfig());

        CacheObjectContext cacheObjCtx = null;

        for (byte[] bytes : byteCol) {
            Object obj = bytes == null ? null : marsh.<T>unmarshal(bytes, ldr0);

            if (obj instanceof Map.Entry) {
                Object key = ((Map.Entry)obj).getKey();

                if (key instanceof KeyCacheObject) {
                    if (cacheObjCtx == null)
                        cacheObjCtx = ctx.cacheContext(cacheId).cacheObjectContext();

                    ((KeyCacheObject)key).finishUnmarshal(cacheObjCtx, ldr0);
                }
            }

            col.add((T)obj);
        }

        return col;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return IndexQuery metadata.
     */
    public IndexQueryResultMeta indexQueryMetadata() {
        return idxQryMetadata;
    }

    /**
     * @param idxQryMetadata IndexQuery metadata.
     */
    public void indexQueryMetadata(IndexQueryResultMeta idxQryMetadata) {
        this.idxQryMetadata = idxQryMetadata;
    }

    /**
     * @return Query data.
     */
    public Collection<Object> data() {
        return data;
    }

    /**
     * @param data Query data.
     */
    public void data(Collection<?> data) {
        this.data = (Collection<Object>)data;
    }

    /**
     * @return If this is last response for this request or not.
     */
    public boolean finished() {
        return finished;
    }

    /**
     * @param finished If this is last response for this request or not.
     */
    public void finished(boolean finished) {
        this.finished = finished;
    }

    /** */
    public Collection<byte[]> dataBytes() {
        return dataBytes;
    }

    /** */
    public void dataBytes(Collection<byte[]> dataBytes) {
        this.dataBytes = dataBytes;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /** */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /** */
    public @Nullable ErrorMessage errorMessage() {
        return errMsg;
    }

    /** */
    public void errorMessage(@Nullable ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /** */
    public void fields(boolean fields) {
        this.fields = fields;
    }

    /**
     * @return If fields query.
     */
    public boolean fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 59;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryResponse.class, this);
    }
}
