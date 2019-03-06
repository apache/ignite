/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Query information (local or distributed).
 */
class GridCacheQueryInfo {
    /** */
    private boolean loc;

    /** */
    private IgniteClosure<?, ?> trans;

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
        IgniteClosure<?, ?> trans,
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
     * @return Transformer.
     */
    IgniteClosure<?, ?> transformer() {
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
