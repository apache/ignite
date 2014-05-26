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
 * Permissions object detached from subject.
 */
public class GridAuthorizationPermissions {
    /** Default allow all flag. */
    private boolean dfltAllowAll;

    /** Task permissions. */
    private Map<String, Collection<GridSecurityOperation>> taskPermissions;

    /** Cache permissions. */
    private Map<String, Collection<GridSecurityOperation>> cachePermissions;

    /**
     * Gets default allow all flag.
     *
     * @return Default allow all flag.
     */
    public boolean defaultAllowAll() {
        return dfltAllowAll;
    }

    /**
     * Sets default allow all flag.
     *
     * @param dfltAllowAll Default allow all flag.
     */
    public void defaultAllowAll(boolean dfltAllowAll) {
        this.dfltAllowAll = dfltAllowAll;
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
