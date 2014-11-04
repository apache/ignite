/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add class description.
 */
public abstract class GridAbstractNearPrepareFuture<K, V> extends GridCompoundIdentityFuture<GridCacheTxEx<K, V>>
    implements GridCacheMvccFuture<K, V, GridCacheTxEx<K, V>> {

    protected GridAbstractNearPrepareFuture() {
    }

    /**
     * @param ctx Context.
     * @param rdc Reducer.
     */
    protected GridAbstractNearPrepareFuture(GridKernalContext ctx,
        @Nullable GridReducer<GridCacheTxEx<K, V>, GridCacheTxEx<K, V>> rdc) {
        super(ctx, rdc);
    }

    public abstract void onResult(UUID nodeId, GridNearTxPrepareResponse<K, V> res);
}
