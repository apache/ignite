/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.hibernate;

import javax.persistence.*;

/**
 * Entry that is used by {@link GridCacheHibernateBlobStore} implementation.
 * <p>
 * Note that this is a reference implementation for tests only.
 * When running on production systems use concrete key-value types to
 * get better performance.
 */
@Entity
@Table(name = "ENTRIES")
public class GridCacheHibernateBlobStoreEntry {
    /** Key (use concrete key type in production). */
    @Id
    @Column(length = 65535)
    private byte[] key;

    /** Value (use concrete value type in production). */
    @Column(length = 65535)
    private byte[] val;

    /**
     * Constructor.
     */
    GridCacheHibernateBlobStoreEntry() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param key Key.
     * @param val Value.
     */
    GridCacheHibernateBlobStoreEntry(byte[] key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    /**
     * @return Key.
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void setKey(byte[] key) {
        this.key = key;
    }

    /**
     * @return Value.
     */
    public byte[] getValue() {
        return val;
    }

    /**
     * @param val Value.
     */
    public void setValue(byte[] val) {
        this.val = val;
    }
}
