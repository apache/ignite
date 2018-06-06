package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.UUID;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Base connection context.
 */
public abstract class ClientListenerAbstractConnectionContext implements ClientListenerConnectionContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected ClientListenerAbstractConnectionContext(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @return Security context.
     */
    @Nullable public SecurityContext securityContext() {
        return secCtx;
    }

    /**
     * Perform authentication.
     *
     * @return Auth context.
     * @throws IgniteCheckedException If failed.
     */
    protected AuthorizationContext authenticate(String user, String pwd) throws IgniteCheckedException {
        AuthorizationContext authCtx;

        if (ctx.security().enabled())
            authCtx = authenticateExternal(user, pwd).authorizationContext();
        else if (ctx.authentication().enabled()) {
            if (F.isEmpty(user))
                throw new IgniteAccessControlException("Unauthenticated sessions are prohibited.");

            authCtx = ctx.authentication().authenticate(user, pwd);

            if (authCtx == null)
                throw new IgniteAccessControlException("Unknown authentication error.");
        }
        else
            authCtx = null;

        return authCtx;
    }

    /**
     * Do 3-rd party authentication.
     */
    private AuthenticationContext authenticateExternal(String user, String pwd) throws IgniteCheckedException {
        SecurityCredentials cred = new SecurityCredentials(user, pwd);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());
        authCtx.nodeAttributes(Collections.emptyMap());
        authCtx.credentials(cred);

        secCtx = ctx.security().authenticate(authCtx);

        if (secCtx == null)
            throw new IgniteAccessControlException(
                String.format("The user name or password is incorrect [userName=%s]", user)
            );

        return authCtx;
    }
}
