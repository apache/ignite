/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.security;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.spi.authentication.*;
import org.gridgain.grid.kernal.managers.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This interface defines a grid authentication manager.
 */
public interface GridSecurityManager extends GridManager {
    /**
     * Authenticates grid node with it's attributes via underlying {@link org.apache.ignite.spi.authentication.AuthenticationSpi}s.
     *
     * @param node Node id to authenticate.
     * @param cred Security credentials.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws IgniteCheckedException If error occurred.
     */
    public GridSecurityContext authenticateNode(ClusterNode node, GridSecurityCredentials cred) throws IgniteCheckedException;

    /**
     * Gets flag indicating whether all nodes or coordinator only should run the authentication for joining node.
     *
     * @return {@code True} if all nodes should run authentication process, {@code false} otherwise.
     */
    public boolean isGlobalNodeAuthentication();

    /**
     * Authenticates subject via underlying {@link org.apache.ignite.spi.authentication.AuthenticationSpi}s.
     *
     * @param ctx Authentication context.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws IgniteCheckedException If error occurred.
     */
    public GridSecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException;

    /**
     * Gets collection of authenticated nodes.
     *
     * @return Collection of authenticated nodes.
     * @throws IgniteCheckedException If error occurred.
     */
    public Collection<GridSecuritySubject> authenticatedSubjects() throws IgniteCheckedException;

    /**
     * Gets authenticated node subject.
     *
     * @param subjId Subject ID.
     * @return Security subject.
     * @throws IgniteCheckedException If error occurred.
     */
    public GridSecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException;

    /**
     * Authorizes grid operation.
     *
     * @param name Cache name or task class name.
     * @param perm Permission to authorize.
     * @param securityCtx Optional security context.
     * @throws GridSecurityException If security check failed.
     */
    public void authorize(String name, GridSecurityPermission perm, @Nullable GridSecurityContext securityCtx)
        throws GridSecurityException;

    /**
     * Callback invoked when subject session got expired.
     *
     * @param subjId Subject ID.
     */
    public void onSessionExpired(UUID subjId);
}
