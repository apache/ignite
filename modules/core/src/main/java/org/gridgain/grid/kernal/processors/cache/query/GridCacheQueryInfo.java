/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
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
