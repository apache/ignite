/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author @java.author
 * @version @java.version
 */
public final class GridUuidCache {
    /** Maximum cache size. */
    private static final int MAX = 1024;

    /** Cache. */
    private static final ConcurrentMap<UUID, UUID> cache =
        new GridBoundedConcurrentLinkedHashMap<>(MAX, 1024, 0.75f, 64);

    /**
     * Gets cached UUID to preserve memory.
     *
     * @param id Read UUID.
     * @return Cached UUID equivalent to the read one.
     */
    public static UUID onGridUuidRead(UUID id) {
        UUID cached = cache.get(id);

        if (cached == null) {
            UUID old = cache.putIfAbsent(id, cached = id);

            if (old != null)
                cached = old;
        }

        return cached;
    }

    /**
     * Ensure singleton.
     */
    private GridUuidCache() {
        // No-op.
    }
}
