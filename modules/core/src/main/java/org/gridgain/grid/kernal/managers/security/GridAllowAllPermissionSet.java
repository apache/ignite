/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.security;

import org.gridgain.grid.security.*;

import java.util.*;

/**
* Allow all permission set.
*/
public class GridAllowAllPermissionSet implements GridSecurityPermissionSet {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean defaultAllowAll() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<GridSecurityPermission>> taskPermissions() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<GridSecurityPermission>> cachePermissions() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridSecurityPermission> systemPermissions() {
        return null;
    }

    /** {@inheritDoc} */
    public String toString() {
        return getClass().getSimpleName();
    }
}
