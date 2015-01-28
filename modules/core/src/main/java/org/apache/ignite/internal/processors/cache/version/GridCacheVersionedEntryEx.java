package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.cache.*;

/**
 *
 */
public interface GridCacheVersionedEntryEx<K, V> extends GridCacheVersionedEntry<K, V>, GridCacheVersionable {
    /**
     *
     * @return
     */
    public boolean isStartVersion();
}
