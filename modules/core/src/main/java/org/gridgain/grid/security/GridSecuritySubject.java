/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import java.util.*;

/**
 * Authenticated security subject.
 *
 * Example JSON configuration:
 * <code>
 *     {
 *         {
 *             "cache": "partitioned",
 *             "permissions": ["CACHE_PUT", "CACHE_READ"]
 *         },
 *         {
 *             "cache": "replicated",
 *             "permissions": ["CACHE_PUT", "CACHE_REMOVE"]
 *         },
 *         {
 *             "task": "org.mytask.*",
 *             "permissions": ["TASK_EXECUTE", "TASK_CANCEL"]
 *         }
 *     }
 * </code>
 */
public class GridSecuritySubject {
    /** Subject ID. */
    private UUID id;

    //TODO subject type

    /** Permissions assigned to a subject. */
    private GridSecurityPermissionSet permissions;

    /**
     * @param id Subject ID.
     */
    public GridSecuritySubject(UUID id) {
        this.id = id;
    }

    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    public UUID id() {
        return id;
    }

    public GridSecurityPermissionSet permissions() {
        return permissions;
    }

    public void permissions(GridSecurityPermissionSet permissions) {
        this.permissions = permissions;
    }
}
