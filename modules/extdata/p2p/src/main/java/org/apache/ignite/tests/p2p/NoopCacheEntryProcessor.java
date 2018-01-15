package org.apache.ignite.tests.p2p;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;

/**
 * @param <K> Key type.
 * @param <V> Value type.
 */
@SuppressWarnings("unchecked")
public class NoopCacheEntryProcessor<K, V> implements CacheEntryProcessor<K, V, Object> {
    /** {@inheritDoc} */
    @Override public Object process(MutableEntry e, Object... args) throws EntryProcessorException {
        e.setValue(args[0]);

        return null;
    }
}
