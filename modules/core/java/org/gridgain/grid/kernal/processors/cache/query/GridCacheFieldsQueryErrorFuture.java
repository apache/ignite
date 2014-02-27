// @java.file.header

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

/**
* Error future for fields query.
*
* @author @java.author
* @version @java.version
*/
public class GridCacheFieldsQueryErrorFuture extends GridCacheQueryErrorFuture<List<?>> {
    /** */
    private boolean incMeta;

    /**
     * @param ctx Context.
     * @param th Error.
     * @param incMeta Include metadata flag.
     */
    public GridCacheFieldsQueryErrorFuture(GridKernalContext ctx, Throwable th, boolean incMeta) {
        super(ctx, th);

        this.incMeta = incMeta;
    }

    /**
     * @return Metadata.
     */
    public GridFuture<List<GridCacheSqlFieldMetadata>> metadata() {
        return new GridFinishedFuture<>(ctx, incMeta ? Collections.<GridCacheSqlFieldMetadata>emptyList() : null);
    }
}
