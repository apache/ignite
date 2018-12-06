package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Default Grid security Manager implementation.
 */
public class GridSecurityManagerImpl implements GridSecurityManager {
    /** Local node's security context. */
    private SecurityContext locSecCtx;

    /** Current security context. */
    private final ThreadLocal<SecurityContext> curSecCtx = ThreadLocal.withInitial(this::localSecurityContext);

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Security processor. */
    private final GridSecurityProcessor secPrc;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /** Map of security contexts. Key is node's id. */
    private final Map<UUID, SecurityContext> secCtxs = new ConcurrentHashMap<>();

    /** Listener, thet removes absolet data from {@link #secCtxs}. */
    private DiscoveryEventListener lsnr;

    /**
     * @param ctx Grid kernal context.
     * @param secPrc Security processor.
     */
    public GridSecurityManagerImpl(GridKernalContext ctx, GridSecurityProcessor secPrc) {
        this.ctx = ctx;
        this.secPrc = secPrc;
        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public GridSecuritySession context(SecurityContext secCtx) {
        assert secCtx != null;

        SecurityContext old = curSecCtx.get();

        curSecCtx.set(secCtx);

        return new GridSecuritySessionImpl(this, old);
    }

    /** {@inheritDoc} */
    @Override public GridSecuritySession context(UUID nodeId) {
        if (lsnr == null) {
            lsnr = (evt, discoCache) -> secCtxs.remove(evt.eventNode().id());

            ctx.event().addDiscoveryEventListener(lsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
        }

        return context(
            secCtxs.computeIfAbsent(nodeId,
                uuid -> nodeSecurityContext(ctx.discovery().node(uuid))
            )
        );
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        return secPrc.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return secPrc.isGlobalNodeAuthentication();
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return secPrc.authenticate(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return secPrc.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return secPrc.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm) throws SecurityException {
        SecurityContext secCtx = curSecCtx.get();

        assert secCtx != null;

        secPrc.authorize(name, perm, secCtx);
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return secPrc.enabled();
    }

    /**
     * Getting local node's security context.
     *
     * @return Security context of local node.
     */
    private SecurityContext localSecurityContext() {
        SecurityContext res = locSecCtx;

        if (res == null) {
            res = nodeSecurityContext(ctx.discovery().localNode());

            locSecCtx = res;
        }

        return res;
    }

    /**
     * Getting node's security context.
     *
     * @param node Node.
     * @return Node's security context.
     */
    private SecurityContext nodeSecurityContext(ClusterNode node) {
        byte[] subjBytes = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);

        byte[] subjBytesV2 = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);

        if (subjBytes == null && subjBytesV2 == null)
            throw new SecurityException("Security context isn't certain.");

        try {
            if (subjBytesV2 != null)
                return U.unmarshal(marsh, subjBytesV2, U.resolveClassLoader(ctx.config()));

            try {
                SecurityUtils.serializeVersion(1);

                return U.unmarshal(marsh, subjBytes, U.resolveClassLoader(ctx.config()));
            }
            finally {
                SecurityUtils.restoreDefaultSerializeVersion();
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to get security context.", e);
        }
    }
}
