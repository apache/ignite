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
 * Local fields query future.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheLocalFieldsQueryFuture
    extends GridCacheLocalQueryFuture<Object, Object, List<Object>>
    implements GridCacheFieldsQueryFuture {
    /** Meta data future. */
    private final GridFutureAdapter<List<GridCacheSqlFieldMetadata>> metaFut;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheLocalFieldsQueryFuture() {
        metaFut = null;
    }

    /**
     * @param ctx Cache context.
     * @param qry Query.
     * @param single Single result or not.
     * @param rmtRdcOnly {@code true} for reduce query when using remote reducer only,
     *     otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     */
    public GridCacheLocalFieldsQueryFuture(GridCacheContext<?, ?> ctx,
        GridCacheFieldsQueryBase qry, boolean single, boolean rmtRdcOnly,
        @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr,
        @Nullable GridPredicate<?> vis) {
        super((GridCacheContext<Object, Object>)ctx, (GridCacheQueryBaseAdapter<Object, Object, GridCacheQueryBase>)qry,
            single, rmtRdcOnly, pageLsnr, vis);

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
    public void onPage(@Nullable UUID nodeId, @Nullable List<GridCacheSqlFieldMetadata> metaData,
        @Nullable Collection<?> data, @Nullable Throwable err, boolean finished) {
        onPage(nodeId, data, err, finished);

        if (!metaFut.isDone())
            metaFut.onDone(metaData);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<List<GridCacheSqlFieldMetadata>> metadata() {
        return metaFut;
    }
}
