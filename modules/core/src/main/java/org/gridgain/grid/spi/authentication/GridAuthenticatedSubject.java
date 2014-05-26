/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication;

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
public class GridAuthenticatedSubject {
    /** Subject ID. */
    private UUID id;

    /** Permissions assigned to a subject. */
    private GridAuthorizationPermissions permissions;

    /**
     * @param id Subject ID.
     */
    public GridAuthenticatedSubject(UUID id) {
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

    public GridAuthorizationPermissions permissions() {
        return permissions;
    }

    public void permissions(GridAuthorizationPermissions permissions) {
        this.permissions = permissions;
    }
}
