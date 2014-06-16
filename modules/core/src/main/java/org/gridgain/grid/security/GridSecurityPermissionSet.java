/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Security permission set for authorized security subjects. Permission set
 * allows to specify task permissions for every task and cache permissions
 * for every cache. While cards are supported at the end of task or
 * cache name.
 * <p>
 * Property {@link #defaultAllowAll()} specifies whether to allow or deny
 * cache and task operations if they were not explicitly specified.
 */
public interface GridSecurityPermissionSet extends Serializable {
    /**
     * Flag indicating whether to allow or deny cache and task operations
     * if they were not explicitly specified.
     *
     * @return {@code True} to allow all cache task operations if they were
     *      not explicitly specified, {@code false} otherwise.
     */
    public boolean defaultAllowAll();

    /**
     * Map of task names to task permissions. Wildcards are allowed at the
     * end of task names.
     *
     * @return Map of task names to task permissions.
     */
    public Map<String, Collection<GridSecurityPermission>> taskPermissions();

    /**
     * Map of cache names to cache permissions. Wildcards are allowed at the
     * end of cache names.
     *
     * @return Map of cache names to cache permissions.
     */
    public Map<String, Collection<GridSecurityPermission>> cachePermissions();

    /**
     * Collection of system-wide permissions (events enable/disable, Visor task execution).
     *
     * @return Collection of system-wide permissions.
     */
    @Nullable public Collection<GridSecurityPermission> systemPermissions();
}
