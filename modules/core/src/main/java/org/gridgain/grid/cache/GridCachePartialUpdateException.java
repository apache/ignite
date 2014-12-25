/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.apache.ignite.*;

import java.util.*;

/**
 * Exception thrown from non-transactional cache in case when update succeeded only partially.
 * One can get list of keys for which update failed with method {@link #failedKeys()}.
 */
public class GridCachePartialUpdateException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Failed keys. */
    private final Collection<Object> failedKeys = new ArrayList<>();

    /**
     * @param msg Error message.
     */
    public GridCachePartialUpdateException(String msg) {
        super(msg);
    }

    /**
     * Gets collection of failed keys.
     * @return Collection of failed keys.
     */
    public <K> Collection<K> failedKeys() {
        return (Collection<K>)failedKeys;
    }

    /**
     * @param failedKeys Failed keys.
     * @param err Error.
     */
    public void add(Collection<?> failedKeys, Throwable err) {
        this.failedKeys.addAll(failedKeys);

        addSuppressed(err);
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return super.getMessage() + ": " + failedKeys;
    }
}
