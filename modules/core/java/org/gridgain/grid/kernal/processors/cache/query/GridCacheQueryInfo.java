// @java.file.header

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Query information (local or distributed).
 *
 * @author @java.author
 * @version @java.version
 */
class GridCacheQueryInfo<K, V> {
    /** */
    private boolean loc;

    /** */
    private GridPredicate<GridCacheEntry<K, V>> prjPred;

    /** */
    private GridClosure<V, Object> trans;

    /** */
    private GridReducer<Map.Entry<K, V>, Object> rdc;

    /** */
    private GridReducer<List<Object>, Object> fieldsRdc;

    /** */
    private GridCacheQueryAdapter<?> qry;

    /** */
    private int pageSize;

    /** */
    private boolean incBackups;

    /** */
    private GridCacheQueryFutureAdapter<K, V, ?> locFut;

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
     * @param fieldsRdc Reducer for fields queries.
     * @param qry Query base.
     * @param pageSize Page size.
     * @param incBackups {@code true} if need to include backups.
     * @param locFut Query future in case of local query.
     * @param sndId Sender node id.
     * @param reqId Request id in case of distributed query.
     * @param incMeta Include meta data or not.
     * @param all Whether to load all pages.
     * @param args Arguments.
     */
    GridCacheQueryInfo(
        boolean loc,
        GridPredicate<GridCacheEntry<K, V>> prjPred,
        GridClosure<V, Object> trans,
        GridReducer<Map.Entry<K, V>, Object> rdc,
        GridReducer<List<Object>, Object> fieldsRdc,
        GridCacheQueryAdapter<?> qry,
        int pageSize,
        boolean incBackups,
        GridCacheQueryFutureAdapter<K, V, ?> locFut,
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
        this.fieldsRdc = fieldsRdc;
        this.qry = qry;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
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
    GridPredicate<GridCacheEntry<K, V>> projectionPredicate() {
        return prjPred;
    }

    /**
     * @return Transformer.
     */
    GridClosure<V, Object> transformer() {
        return trans;
    }

    /**
     * @return Reducer.
     */
    GridReducer<Map.Entry<K, V>, Object> reducer() {
        return rdc;
    }

    /**
     * @return Reducer for fields queries.
     */
    GridReducer<List<Object>, Object> fieldsReducer() {
        return fieldsRdc;
    }

    /**
     * @return Page size.
     */
    int pageSize() {
        return pageSize;
    }

    /**
     * @return {@code true} if need to include backups.
     */
    boolean includeBackups() {
        return incBackups;
    }

    /**
     * @return Query future in case of local query.
     */
    @Nullable GridCacheQueryFutureAdapter<K, V, ?> localQueryFuture() {
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
