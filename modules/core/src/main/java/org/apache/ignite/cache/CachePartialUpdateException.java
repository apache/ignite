/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache;

import org.gridgain.grid.cache.*;

import javax.cache.*;
import java.util.*;

/**
 * Exception thrown from non-transactional cache in case when update succeeded only partially.
 * One can get list of keys for which update failed with method {@link #failedKeys()}.
 */
public class CachePartialUpdateException extends CacheException {
    /**
     * @param e Cause.
     */
    public CachePartialUpdateException(GridCachePartialUpdateException e) {
        super(e.getMessage(), e);
    }

    /**
     * Gets collection of failed keys.
     * @return Collection of failed keys.
     */
    public <K> Collection<K> failedKeys() {
        return ((GridCachePartialUpdateException)getCause()).failedKeys();
    }
}
