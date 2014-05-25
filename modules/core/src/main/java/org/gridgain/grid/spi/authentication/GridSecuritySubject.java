/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication;

import org.gridgain.grid.security.*;

import java.util.*;

/**
 * Authenticated security subject.
 *
 * Example JSON configuration:
 * <code>
 *     {
 *         {
 *             "cache": "partitioned",
 *             "permissions": ["PUT", "READ"]
 *         }
 *         {
 *             "cache": "replicated",
 *             "permissions": ["PUT", "REMOVE"]
 *         }
 *         allow-tasks: [
 *             "package.name.Task",
 *             "package.wildcard.*"
 *         ]
 *     }
 * </code>
 */
public interface GridSecuritySubject {
    /**
     * Gets allowed permissions for cache with given name.
     *
     * @param cacheName Cache name.
     * @return Collection of operations allowed for this cache.
     */
    public Collection<GridSecurityOperation> permissions(String cacheName);

    /**
     * Gets flag indicating whether task execution with given class name is allowed.
     *
     * @param taskClsName Task class name to check.
     * @return {@code True} if execution is allowed, {@code false} otherwise.
     */
    public boolean executionAllowed(String taskClsName);
}
