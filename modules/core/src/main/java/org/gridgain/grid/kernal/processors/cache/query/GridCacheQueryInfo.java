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

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Query information (local or distributed).
 */
class GridCacheQueryInfo {
    /** */
    private boolean loc;

    /** */
    private IgnitePredicate<GridCacheEntry<Object, Object>> prjPred;

    /** */
    private IgniteClosure<Object, Object> trans;

    /** */
    private IgniteReducer<Object, Object> rdc;

    /** */
    private GridCacheQueryAdapter<?> qry;

    /** */
    private GridCacheLocalQueryFuture<?, ?, ?> locFut;

    /** */
    private UUID sndId;

    /** */
    private long reqId;

    /** */
    private boolean incMeta;

    /** */
    private boolean all;

    /** */
    private Object[] args;

    /**
     * @param loc {@code true} if local query.
     * @param prjPred Projection predicate.
     * @param trans Transforming closure.
     * @param rdc Reducer.
     * @param qry Query base.
     * @param locFut Query future in case of local query.
     * @param sndId Sender node id.
     * @param reqId Request id in case of distributed query.
     * @param incMeta Include meta data or not.
     * @param all Whether to load all pages.
     * @param args Arguments.
     */
    GridCacheQueryInfo(
        boolean loc,
        IgnitePredicate<GridCacheEntry<Object, Object>> prjPred,
        IgniteClosure<Object, Object> trans,
        IgniteReducer<Object, Object> rdc,
        GridCacheQueryAdapter<?> qry,
        GridCacheLocalQueryFuture<?, ?, ?> locFut,
        UUID sndId,
        long reqId,
        boolean incMeta,
        boolean all,
        Object[] args
    ) {
        this.loc = loc;
        this.prjPred = prjPred;
        this.trans = trans;
        this.rdc = rdc;
        this.qry = qry;
        this.locFut = locFut;
        this.sndId = sndId;
        this.reqId = reqId;
        this.incMeta = incMeta;
        this.all = all;
        this.args = args;
    }

    /**
     * @return Local or not.
     */
    boolean local() {
        return loc;
    }

    /**
     * @return Id of sender node.
     */
    @Nullable UUID senderId() {
        return sndId;
    }

    /**
     * @return Query.
     */
    GridCacheQueryAdapter<?> query() {
        return qry;
    }

    /**
     * @return Projection predicate.
     */
    IgnitePredicate<GridCacheEntry<Object, Object>> projectionPredicate() {
        return prjPred;
    }

    /**
     * @return Transformer.
     */
    IgniteClosure<?, Object> transformer() {
        return trans;
    }

    /**
     * @return Reducer.
     */
    IgniteReducer<?, Object> reducer() {
        return rdc;
    }

    /**
     * @return Query future in case of local query.
     */
    @Nullable GridCacheLocalQueryFuture<?, ?, ?> localQueryFuture() {
        return locFut;
    }

    /**
     * @return Request id in case of distributed query.
     */
    long requestId() {
        return reqId;
    }

    /**
     * @return Include meta data or not.
     */
    boolean includeMetaData() {
        return incMeta;
    }

    /**
     * @return Whether to load all pages.
     */
    boolean allPages() {
        return all;
    }

    /**
     * @return Arguments.
     */
    Object[] arguments() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryInfo.class, this);
    }
}
