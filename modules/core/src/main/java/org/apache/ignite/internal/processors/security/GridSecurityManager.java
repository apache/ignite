package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Grid Security Manager.
 */
public interface GridSecurityManager {
    /**
     * Create {@link GridSecuritySession}. All calls of methods {@link #authorize(String, SecurityPermission)} or {@link
     * #authorize(SecurityPermission)} will be processed into the context of passed {@link SecurityContext} until
     * session {@link GridSecuritySession} will be closed.
     *
     * @param secCtx Security Context.
     * @return Grid security Session.
     */
    public GridSecuritySession startSession(SecurityContext secCtx);

    /**
     * Create {@link GridSecuritySession}. All calls of methods {@link #authorize(String, SecurityPermission)} or {@link
     * #authorize(SecurityPermission)} will be processed into the context of {@link SecurityContext} that is owned by
     * node with given noddeId until session {@link GridSecuritySession} will be closed.
     *
     * @param nodeId Node id.
     * @return Grid security Session.
     */
    public GridSecuritySession startSession(UUID nodeId);

    /**
     * Delegate call to {@link GridSecurityProcessor#authenticateNode(org.apache.ignite.cluster.ClusterNode,
     * org.apache.ignite.plugin.security.SecurityCredentials)}
     */
    public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException;

    /**
     * Delegate call to {@link GridSecurityProcessor#isGlobalNodeAuthentication()}
     */
    public boolean isGlobalNodeAuthentication();

    /**
     * Delegate call to {@link GridSecurityProcessor#authenticate(AuthenticationContext)}
     */
    public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException;

    /**
     * Delegate call to {@link GridSecurityProcessor#authenticatedSubjects()}
     */
    public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException;

    /**
     * Delegate call to {@link GridSecurityProcessor#authenticatedSubject(UUID)}
     */
    public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException;

    /**
     * Authorizes grid operation.
     *
     * @param name Cache name or task class name.
     * @param perm Permission to authorize.
     * @throws SecurityException If security check failed.
     */
    public void authorize(String name, SecurityPermission perm) throws SecurityException;

    /**
     * Authorizes grid system operation.
     *
     * @param perm Permission to authorize.
     * @throws SecurityException If security check failed.
     */
    public default void authorize(SecurityPermission perm) throws SecurityException {
        authorize(null, perm);
    }

    /**
     * Delegate call to {@link GridSecurityProcessor#enabled()}
     */
    public boolean enabled();
}
