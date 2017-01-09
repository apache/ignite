package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Entry processor for SQL UPDATE operation.
 */
public final class UpdateProcessor extends DmlEntryProcessor {
    /** Values to set - in order specified by {@link DmlEntryProcessorArgs#props}. */
    private final Object[] newColVals;

    /** New value, if given in initial query. */
    private final Object newVal;

    /** */
    public UpdateProcessor(Object[] newColVals, Object newVal) {
        this.newColVals = newColVals;
        this.newVal = newVal;
    }

    /** {@inheritDoc} */
    @Override public void applyx(CacheInvokeEntry<Object, Object> e, DmlEntryProcessorArgs args) throws IgniteCheckedException {
        assert e.exists();

        GridCacheContext cctx = e.entry().context();

        GridQueryTypeDescriptor typeDesc = cctx.grid().context().query().type(cctx.name(), args.typeName);

        // If we're here then value check has passed, so we can take old val as basis.
        Object val = U.firstNotNull(newVal, e.oldVal());

        boolean hasProps = (newVal == null || newColVals.length > 1);

        val = hasProps ? toBuilderIfNeeded(cctx, val) : val;

        if (val == null)
            throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

        int i = 0;

        for (String propName : typeDesc.fields().keySet()) {
            Integer idx = args.props.get(i++);

            if (idx == null)
                continue;

            GridQueryProperty prop = typeDesc.property(propName);

            assert !prop.key();

            Object colVal = newColVals[idx];

            prop.setValue(null, val, colVal);
        }

        if (cctx.binaryMarshaller() && hasProps) {
            assert val instanceof BinaryObjectBuilder;

            val = ((BinaryObjectBuilder) val).build();

            val = DmlStatementsProcessor.updateHashCodeIfNeeded(cctx, (BinaryObject) val);
        }

        e.setValue(val);
    }
}
