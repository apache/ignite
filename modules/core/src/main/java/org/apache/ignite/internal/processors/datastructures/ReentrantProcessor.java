package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

public abstract class ReentrantProcessor<T>
    implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<T>, Boolean>,
    Externalizable {

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<T>> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Base<T> state = entry.getValue();

            LockedModified result = tryLock(state);

            // Write result if necessary
            if (result.modified) {
                entry.setValue(state);
            }

            return result.locked;
        } //else System.out.println("!!!~ куда-то съебалась запись 2!");

        return false;
    }

    protected abstract  LockedModified tryLock(GridCacheLockState2Base<T> state);
}
