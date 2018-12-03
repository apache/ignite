/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Wrapper that provides getting current security context for the {@link GridSecurityProcessor#authorize(String,
 * SecurityPermission, SecurityContext)} method.
 */
public class SecurityContextResolverSecurityProcessor extends GridSecurityProcessorWrapper {
    /** Local node's security context. */
    private SecurityContext locSecCtx;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /** Map of security contexts. Key is node's id. */
    private final Map<UUID, SecurityContext> secCtxs = new ConcurrentHashMap<>();

    /**
     * @param ctx Grid kernal context.
     * @param original Original grid security processor.
     */
    public SecurityContextResolverSecurityProcessor(GridKernalContext ctx, GridSecurityProcessor original) {
        super(ctx, original);

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {
        if (enabled()) {
            SecurityContext curSecCtx = securityCtx(securityCtx);

            if (log.isDebugEnabled())
                log.debug("Authorize [name=" + name + ", perm=" + perm + "secCtx=" + curSecCtx + ']');

            original.authorize(name, perm, curSecCtx);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        if (enabled()) {
            ctx.event().addDiscoveryEventListener(new DiscoveryEventListener() {
                @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
                    secCtxs.remove(evt.eventNode().id());
                }
            }, EVT_NODE_FAILED, EVT_NODE_LEFT);
        }
    }

    /**
     * Get current initiator data.
     *
     * @return Initiator node's id.
     */
    private RemoteInitiator currentRemoteInitiator() {
        return CurrentRemoteInitiator.get();
    }

    /**
     * Getting current security context.
     *
     * @param passed Security context that was passed to {@link #authorize(String, SecurityPermission,
     * SecurityContext)}.
     * @return Current security context.
     */
    private SecurityContext securityCtx(SecurityContext passed) {
        SecurityContext res = passed;

        if (res == null) {
            res = currentRemoteInitiatorContext();

            if (res == null)
                res = localSecurityContext();
        }
        else {
            /*If the SecurityContext was passed and there is a current remote initiator
            then we have got an invalid case.*/
            assert currentRemoteInitiator() == null;
        }

        assert res != null;

        return res;
    }

    /**
     * Getting current initiator node's security context.
     *
     * @return Security context of initiator node.
     */
    private SecurityContext currentRemoteInitiatorContext() {
        SecurityContext secCtx = null;

        final RemoteInitiator cur = currentRemoteInitiator();

        if (cur != null) {
            if (cur.securityContext() != null)
                secCtx = cur.securityContext();
            else {
                UUID nodeId = cur.nodeId();

                secCtx = secCtxs.computeIfAbsent(nodeId,
                    new Function<UUID, SecurityContext>() {
                        @Override public SecurityContext apply(UUID uuid) {
                            return nodeSecurityContext(ctx.discovery().node(nodeId));

                        }
                    });
            }
        }

        return secCtx;
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
