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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/**
 *
 */
public final class IgniteCompoundDiagnosicInfo implements Message {
    /** ID of node sent info. */
    @Order(0)
    private UUID nodeId;

    /** Info to send on remote node. */
    private final Set<IgniteDiagnosticMessage.DiagnosticBaseInfo> info = new LinkedHashSet<>();

    /** {@code info} serialization call holder. */
    @Order(1)
    private Collection<byte[]> infosBytes;

    /** Local message related to remote info. */
    private Map<Object, List<String>> msgs = new LinkedHashMap<>();

    /** Empty constructor required by {@link Externalizable}. */
    public IgniteCompoundDiagnosicInfo() {
        // No-op.
    }

    /**
     * @param nodeId ID of node sent info.
     */
    IgniteCompoundDiagnosicInfo(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @param ctx Grid context.
     * @return Diagnostic info.
     */
    public String diagnosticInfo(GridKernalContext ctx) {
        try {
            IgniteInternalFuture<String> commInfo = IgniteDiagnosticMessage.dumpCommunicationInfo(ctx, nodeId);

            StringBuilder sb = new StringBuilder();

            IgniteDiagnosticMessage.dumpNodeBasicInfo(sb, ctx);

            sb.append(U.nl());

            IgniteDiagnosticMessage.dumpExchangeInfo(sb, ctx);

            sb.append(U.nl());

            IgniteDiagnosticMessage.dumpPendingCacheMessages(sb, ctx);

            sb.append(commInfo.get(10_000));

            moreInfo(sb, ctx);

            return sb.toString();
        }
        catch (Exception e) {
            ctx.cluster().diagnosticLog().error("Failed to execute diagnostic message closure: " + e, e);

            return "Failed to execute diagnostic message closure: " + e;
        }
    }

    /**
     * @param sb String builder.
     * @param ctx Grid context.
     */
    private void moreInfo(StringBuilder sb, GridKernalContext ctx) {
        for (IgniteDiagnosticMessage.DiagnosticBaseInfo baseInfo : info) {
            try {
                baseInfo.appendInfo(sb, ctx);
            }
            catch (Exception e) {
                ctx.cluster().diagnosticLog().error(
                    "Failed to populate diagnostic with additional information: " + e, e);

                sb.append(U.nl()).append("Failed to populate diagnostic with additional information: ").append(e);
            }
        }
    }

    /**
     * @return Initial message.
     */
    public String message() {
        StringBuilder sb = new StringBuilder();

        for (List<String> msgs0 : msgs.values()) {
            for (String msg : msgs0) {
                if (sb.length() > 0)
                    sb.append('\n');

                sb.append(msg);
            }
        }

        return sb.toString();
    }

    /**
     * @param msg Message.
     * @param baseInfo Info or {@code null} if only basic info is needed.
     */
    public void add(String msg, @Nullable IgniteDiagnosticMessage.DiagnosticBaseInfo baseInfo) {
        Object key = baseInfo != null ? baseInfo : getClass();

        msgs.computeIfAbsent(key, k -> new ArrayList<>()).add(msg);

        if (baseInfo != null) {
            if (!info.add(baseInfo) && baseInfo instanceof TxEntriesInfo) {
                for (IgniteDiagnosticMessage.DiagnosticBaseInfo baseInfo0 : info) {
                    if (baseInfo0.equals(baseInfo))
                        baseInfo0.merge(baseInfo);
                }
            }
        }
    }

    /** Deserealizes {@code info}. */
    public void infosBytes(@Nullable Collection<byte[]> infosBytes) {
        info.clear();

        if (F.isEmpty(infosBytes))
            return;

        try {
            for (byte[] bytes : infosBytes)
                info.add(U.unmarshal(jdk(), bytes, U.gridClassLoader()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to deserialize a diagnostic info.", e);
        }
    }

    /** Serealizes {@code info}. */
    public @Nullable Collection<byte[]> infosBytes() {
        if (info.isEmpty())
            return null;

        Collection<byte[]> res = new ArrayList<>(info.size());

        try {
            for (IgniteDiagnosticMessage.DiagnosticBaseInfo i : info)
                res.add(U.marshal(jdk(), i));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to serialize a diagnostic info.", e);
        }

        return res;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    @Override public short directType() {
        return -62;
    }
}
