/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication;

import org.gridgain.grid.security.*;

import java.net.*;
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

    /** Task permissions. */
    private Map<String, Collection<GridSecurityOperation>> taskPermissions;

    /** Cache permissions. */
    private Map<String, Collection<GridSecurityOperation>> cachePermissions;

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

    /**
     * Gets mapping from task name mask to allowed operations.
     *
     * @return Mapping from task name to collection of permitted operations.
     */
    public Map<String, Collection<GridSecurityOperation>> taskPermissions() {
        return taskPermissions;
    }

    /**
     * Sets mapping from task name mask to allowed operations.
     *
     * @param taskPermissions Mapping from task name to collection of permitted operations.
     */
    public void taskPermissions(Map<String, Collection<GridSecurityOperation>> taskPermissions) {
        this.taskPermissions = taskPermissions;
    }

    /**
     * Gets mapping from cache name mask to collection of allowed operations.
     *
     * @return Mapping from cache name to collection of permitted operations.
     */
    public Map<String, Collection<GridSecurityOperation>> cachePermissions() {
        return cachePermissions;
    }

    /**
     * Sets mapping from cache name mask to collection of allowed operations.
     *
     * @param cachePermissions Mapping from cache name to collection of allowed operations.
     */
    public void cachePermissions(Map<String, Collection<GridSecurityOperation>> cachePermissions) {
        this.cachePermissions = cachePermissions;
    }
}
