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
import org.apache.ignite.internal.util.typedef.F;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * Entry processor that performs INSERT on an entry.
 */
public class InsertProcessor extends DmlEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
    /** New values for properties as enlisted in initial query (w/o key and its properties, as well as _val). */
    private final Object[] newColVals;

    /** New _val, if present in initial query. */
    private final Object newVal;

    /** */
    public InsertProcessor(Object[] newColVals, Object newVal) {
        this.newColVals = newColVals;
        this.newVal = newVal;
    }

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<Object, Object> e, Object... args) throws EntryProcessorException {
        if (e.exists())
            return false;

        if (!(e instanceof CacheInvokeEntry))
            throw new EntryProcessorException("Unexpected mutable entry type - CacheInvokeEntry expected");

        assert !F.isEmpty(args) && args[0] instanceof DmlEntryProcessorArgs;

        try {
            applyx((CacheInvokeEntry<Object, Object>) e, (DmlEntryProcessorArgs) args[0]);
        }
        catch (IgniteCheckedException ex) {
            throw new EntryProcessorException(ex);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void applyx(CacheInvokeEntry<Object, Object> e, DmlEntryProcessorArgs args) throws IgniteCheckedException {
        GridCacheContext cctx = e.entry().context();

        GridQueryTypeDescriptor typeDesc = cctx.grid().context().query().type(cctx.name(), args.typeName);

        Object val = null;

        if (newVal != null)
            val = !F.isEmpty(newColVals) ? DmlEntryProcessor.toBuilderIfNeeded(cctx, newVal) : newVal;
        else if (cctx.binaryMarshaller()) // For non binary mode, newVal must be supplied.
            val = cctx.grid().binary().builder(typeDesc.valueTypeName());

        if (val == null)
            throw new IgniteSQLException("Value for INSERT or MERGE must not be null",
                IgniteQueryErrorCode.NULL_VALUE);

        int i = 0;

        if (!F.isEmpty(newColVals))
            for (String propName : typeDesc.fields().keySet()) {
                Integer idx = args.props.get(i++);

                if (idx == null)
                    continue;

                GridQueryProperty prop = typeDesc.property(propName);

                if (prop.key())
                    continue;

                prop.setValue(null, val, newColVals[idx]);
            }

        if (cctx.binaryMarshaller() && newVal == null) {
            val = ((BinaryObjectBuilder) val).build();

            val = DmlStatementsProcessor.updateHashCodeIfNeeded(cctx, (BinaryObject) val);
        }

        e.setValue(val);
    }
}
