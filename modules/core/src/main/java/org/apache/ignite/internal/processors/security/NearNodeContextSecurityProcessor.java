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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Wrapper that provides getting current security
 * context for the {@link GridSecurityProcessor#authorize(String, SecurityPermission, SecurityContext)} method.
 */
public class NearNodeContextSecurityProcessor extends GridSecurityProcessorWrp {
    /** Near node's id. */
    private final ThreadLocal<UUID> nearNodeId = new ThreadLocal<>();
    //todo сюда нужно ещё мапу воткнуть для сохранения контекста удаленной ноды.
    //чистить его по событию покидания узлом топологии.
    /** Local node's security context. */
    private SecurityContext locSecCtx;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /**
     * @param ctx Grid kernal context.
     * @param original Original grid security processor.
     */
    public NearNodeContextSecurityProcessor(GridKernalContext ctx, GridSecurityProcessor original) {
        super(ctx, original);

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {
        original.authorize(name, perm, securityCtx(securityCtx));
    }

    /**
     * Set near node's id.
     *
     * @param id Near node's id.
     */
    public void nearNodeId(UUID id) {
        nearNodeId.set(id);
    }

    /**
     * Get near dode's id.
     *
     * @return Near node's id.
     */
    public UUID nearNodeId() {
        return nearNodeId.get();
    }

    /**
     * Remove near node's id.
     */
    public void removeNearNodeId() {
        nearNodeId.remove();
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
        try {
            if (res == null) {
                res = nearNodeSecurityCtx();

                if (res == null)
                    res = localSecurityCtx();
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to get security context.", e);
        }

        return res;
    }

    /**
     * Getting current near node's security context.
     *
     * @return Security context of near node.
     */
    private SecurityContext nearNodeSecurityCtx() throws IgniteCheckedException {
        SecurityContext secCtx = null;

        UUID nodeId = nearNodeId();

        if (nodeId != null)
            secCtx = nodeSecurityCtx(ctx.discovery().node(nodeId));

        return secCtx;
    }

    /**
     * Getting local node's security context.
     *
     * @return Security context of local node.
     * @throws IgniteCheckedException If error occurred.
     */
    private SecurityContext localSecurityCtx() throws IgniteCheckedException {
        SecurityContext res = locSecCtx;

        if (res == null) {
            res = nodeSecurityCtx(ctx.discovery().localNode());

            locSecCtx = res;
        }

        assert res != null;

        return res;
    }

    /**
     * Getting node's security context.
     *
     * @param node Node.
     * @return Node's security context.
     * @throws IgniteCheckedException If failed.
     */
    private SecurityContext nodeSecurityCtx(ClusterNode node) throws IgniteCheckedException {
        byte[] subjBytes = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);

        byte[] subjBytesV2 = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);

        if (subjBytes == null && subjBytesV2 == null)
             throw new SecurityException("Local security context isn't certain.");

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
}
