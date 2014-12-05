/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.authentication;

import org.gridgain.grid.*;
import org.apache.ignite.plugin.security.*;

import java.util.*;

/**
 * Access control list provider. Specific SPI implementation may use this
 * interface for declarative user permission specifications.
 * <p>
 * Abstracting access control specification through a provider allows users
 * to implement custom stores for per-user access control specifications,
 * e.g. encrypting them or storing in a separate file.
 */
public interface AuthenticationAclProvider {
    /**
     * Gets per-user access control map.
     *
     * @return Per-user access control map.
     * @throws GridException If failed.
     */
    public Map<GridSecurityCredentials, GridSecurityPermissionSet> acl() throws GridException;
}
