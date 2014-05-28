// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.security;

import org.gridgain.grid.security.*;

/**
 * Security context.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridSecurityContext {
    /** Security subject. */
    private GridSecuritySubject subj;

    /**
     * @param subj Subject.
     */
    public GridSecurityContext(GridSecuritySubject subj) {
        this.subj = subj;

        // TODO init permissions rules.
    }

    /**
     * @return Security subject.
     */
    public GridSecuritySubject subject() {
        return subj;
    }

    /**
     * Checks whether task operation is allowed.
     *
     * @param taskClsName Task class name.
     * @param perm Permission to check.
     * @return {@code True} if task operation is allowed.
     */
    public boolean taskOperationAllowed(String taskClsName, GridSecurityPermission perm) {
        return true;
    }

    /**
     * Checks whether cache operation is allowed.
     *
     * @param cacheName Cache name.
     * @param perm Permission to check.
     * @return {@code True} if cache operation is allowed.
     */
    public boolean cacheOperationAllowed(String cacheName, GridSecurityPermission perm) {
        return true;
    }
}
