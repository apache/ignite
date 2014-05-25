/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

/**
 * Security configuration.
 */
public class GridSecurityConfiguration {
    /** Default value for default allow all flag. */
    public static final boolean DFLT_ALLOW_ALL = true;

    /** Default allow all flag. */
    private boolean defaultAllowAll = DFLT_ALLOW_ALL;

    /** Security credentials. */
    private GridSecurityCredentials cred;

    /**
     * If set to false, tasks and cache operations that are not explicitly allowed, will be denied.
     *
     * @return {@code True} if task execution and cache operations should be allowed by default.
     */
    public boolean isDefaultAllowAll() {
        return defaultAllowAll;
    }

    /**
     * Gets default allow all flag value.
     *
     * @param defaultAllowAll {@code True} if task execution and cache operations should be allowed by default.
     */
    public void setDefaultAllowAll(boolean defaultAllowAll) {
        this.defaultAllowAll = defaultAllowAll;
    }

    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     */
    public GridSecurityCredentials getCredentials() {
        return cred;
    }

    /**
     * Sets security credentials.
     *
     * @param cred Security credentials.
     */
    public void setCredentials(GridSecurityCredentials cred) {
        this.cred = cred;
    }
}
