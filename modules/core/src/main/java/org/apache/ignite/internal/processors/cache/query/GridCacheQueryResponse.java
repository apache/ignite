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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMeta;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.DeployableMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Page of cache query response.
 */
public class GridCacheQueryResponse extends GridCacheIdMessage implements GridCacheDeployable, MarshallableMessage, DeployableMessage {
    /** */
    @Order(0)
    boolean finished;

    /** */
    @Order(1)
    long reqId;

    /** */
    @Order(2)
    @Nullable ErrorMessage errMsg;

    /** */
    @Order(3)
    boolean fields;

    /** */
    @Order(4)
    IndexQueryResultMeta idxQryMetadata;

    /** */
    @Order(5)
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
     */
    public GridCacheQueryResponse(int cacheId, long reqId, boolean finished, boolean fields) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.finished = finished;
        this.fields = fields;
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

    /** @return If this is last response for this request or not. */
    public boolean finished() {
        return finished;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @return If fields query.
     */
    public boolean fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (data != null)
            dataBytes = marshallCollection(data, marsh);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (dataBytes != null)
            data = unmarshalCollection(dataBytes, marsh, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public void prepareDeployment(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (dataBytes == null && data != null)
            GridCacheMessageDeployer.prepareCollection(this, data, cctx);

        if (addDepInfo && !F.isEmpty(data)) {
            for (Object o : data) {
                if (o instanceof Map.Entry) {
                    Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;

                    GridCacheMessageDeployer.prepareObject(this, e.getKey(), cctx);
                    GridCacheMessageDeployer.prepareObject(this, e.getValue(), cctx);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryResponse.class, this);
    }
}
