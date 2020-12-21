package org.apache.ignite.internal.cache.query.index.sorted;

/**
 * Represents null value that used to explicitly set up null value in index query.
 */
public class NullKey<K> {
    /** Instance. */
    public static final NullKey INSTANCE = new NullKey();

    /** Private constructor. */
    private NullKey() {};
}
