package org.apache.ignite.platform;

import org.apache.ignite.cache.CacheEntryEventSerializableFilter;

/**
 * Test filter factory
 */
public class PlatformCacheEntryEventFilterFactory implements PlatformJavaObjectFactory<CacheEntryEventSerializableFilter> {
    /** Property to be set from platform. */
    @SuppressWarnings("FieldCanBeLocal")
    private String startsWith = "-";

    /** {@inheritDoc} */
    @Override public CacheEntryEventSerializableFilter create() {
        return null;
    }
}
