package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.CIX2;

/**
 * In-closure that mutates given entry with respect to given args.
 */
public abstract class DmlEntryProcessor extends CIX2<CacheInvokeEntry<Object, Object>, DmlEntryProcessorArgs> {
    /**
     * Optionally wrap an object into {@link BinaryObjectBuilder}.
     *
     * @param cctx Cache context.
     * @param val Value to wrap.
     * @return {@code val} or {@link BinaryObjectBuilder} wrapping it.
     */
    static Object toBuilderIfNeeded(GridCacheContext cctx, Object val) {
        if (val == null || !cctx.binaryMarshaller() || GridQueryProcessor.isSqlType(val.getClass()))
            return val;

        BinaryObject binVal = cctx.grid().binary().toBinary(val);

        return binVal.toBuilder();
    }
}
