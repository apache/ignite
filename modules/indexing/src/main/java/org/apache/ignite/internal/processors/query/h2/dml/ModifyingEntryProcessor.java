package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.util.typedef.F;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;

/**
 * Entry processor invoked by DML operations - essentially a wrapper for a {@link DmlEntryProcessor} closure.
 */
public final class ModifyingEntryProcessor implements EntryProcessor<Object, Object, Boolean>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value to expect. */
    private final Object val;

    /** Action to perform on entry. */
    private final DmlEntryProcessor entryModifier;

    /** */
    public ModifyingEntryProcessor(Object val, DmlEntryProcessor entryModifier) {
        assert val != null;

        this.val = val;
        this.entryModifier = entryModifier;
    }

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments)
        throws EntryProcessorException {
        if (!entry.exists())
            return null; // Someone got ahead of us and removed this entry, let's skip it.

        Object entryVal = entry.getValue();

        if (entryVal == null)
            return null;

        // Something happened to the cache while we were performing map-reduce.
        if (!F.eq(entryVal, val))
            return false;

        if (!(entry instanceof CacheInvokeEntry))
            throw new EntryProcessorException("Unexpected mutable entry type - CacheInvokeEntry expected");

        boolean hasArgs = !F.isEmpty(arguments);

        assert !hasArgs || (arguments[0] != null && arguments[0] instanceof DmlEntryProcessorArgs);

        try {
            entryModifier.apply((CacheInvokeEntry<Object, Object>) entry,
                hasArgs ? (DmlEntryProcessorArgs) arguments[0] : null);
        }
        catch (Exception e) {
            throw new EntryProcessorException(e);
        }

        return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
    }
}
