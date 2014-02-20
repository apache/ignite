// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Distributed fields query future.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheDistributedFieldsQueryFuture
    extends GridCacheDistributedQueryFuture<Object, Object, List<Object>>
    implements GridCacheFieldsQueryFuture {
    /** Meta data future. */
    private final GridFutureAdapter<List<GridCacheQueryFieldDescriptor>> metaFut;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheDistributedFieldsQueryFuture() {
        metaFut = null;
    }

    /**
     * @param ctx Cache context.
     * @param reqId Request ID.
     * @param qry Query.
     * @param nodes Nodes.
     * @param single Single result or not.
     * @param rmtRdcOnly {@code true} for reduce query when using remote reducer only,
     *     otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     */
    public GridCacheDistributedFieldsQueryFuture(GridCacheContext<?, ?> ctx, long reqId,
        GridCacheFieldsQuery qry, Iterable<GridNode> nodes, boolean single, boolean rmtRdcOnly,
        @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr,
        @Nullable GridPredicate<?> vis) {
        super((GridCacheContext<Object, Object>)ctx, reqId, (GridCacheQueryBaseAdapter<Object, Object>)qry,
            nodes, single, rmtRdcOnly, pageLsnr, vis);

        metaFut = new GridFutureAdapter<>(ctx.kernalContext());

        if (!qry.includeMetadata())
            metaFut.onDone();
    }

    /**
     * @param nodeId Sender node ID.
     * @param metaData Meta data.
     * @param data Page data.
     * @param err Error.
     * @param finished Finished or not.
     */
    public void onPage(@Nullable UUID nodeId, @Nullable List<GridCacheQueryFieldDescriptor> metaData,
        @Nullable Collection<Map<String, Object>> data, @Nullable Throwable err, boolean finished) {
        if (!metaFut.isDone() && metaData != null)
            metaFut.onDone(metaData);

        onPage(nodeId, data, err, finished);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Collection<List<Object>> res, @Nullable Throwable err) {
        if (!metaFut.isDone())
            metaFut.onDone();

        return super.onDone(res, err);
    }

    /** {@inheritDoc} */
    @Override public boolean onCancelled() {
        if (!metaFut.isDone())
            metaFut.onDone();

        return super.onCancelled();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<List<GridCacheQueryFieldDescriptor>> metadata() {
        return metaFut;
    }
}
