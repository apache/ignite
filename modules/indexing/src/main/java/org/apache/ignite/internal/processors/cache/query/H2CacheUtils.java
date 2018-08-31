package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Utilities related to H2 cache
 */
public class H2CacheUtils {

    /**
     *
     */
    private H2CacheUtils() {
    }

    /**
     * Check that given table has lazy cache and init it for such case.
     *
     * @param tbl Table to check on lazy cache
     */
    public static void checkAndInitLazyCache(GridH2Table tbl) {
        if(tbl.isCacheLazy()){
            String cacheName = tbl.cacheInfo().config().getName();

            GridKernalContext ctx = tbl.cacheInfo().context();

            try {
                ctx.cache().dynamicStartCache(null, cacheName, null, false, true, true).get();
            } catch(IgniteCheckedException ex){
                throw U.convertException(ex);
            }
        }
    }
}
