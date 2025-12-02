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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteDiagnosticRequest implements Message {
    /** */
    @Order(value = 0, method = "futureId")
    private long futId;

    /** Originator node id. */
    @Order(1)
    private UUID nodeId;

    /** Infos to send to a remote node. */
    @Order(2)
    private Collection<DiagnosticBaseInfo> infos;

    /** Local message related to remote info. */
    private final Map<Object, List<String>> msgs = new LinkedHashMap<>();

    /**
     * Default constructor required by {@link GridIoMessageFactory}.
     */
    public IgniteDiagnosticRequest() {
        // No-op.
    }

    /**
     * Creates a diagnostic info holder.
     *
     * @param nodeId Originator node ID.
     */
    IgniteDiagnosticRequest(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Creates a diagnostic request.
     *
     * @param futId Future ID.
     * @param nodeId Node ID.
     * @param infos Diagnostic infos.
     */
    public IgniteDiagnosticRequest(long futId, UUID nodeId, Collection<DiagnosticBaseInfo> infos) {
        this(nodeId);

        assert infos != null;

        this.futId = futId;
        this.infos = new LinkedHashSet<>(infos);
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
    void add(String msg, @Nullable IgniteDiagnosticRequest.DiagnosticBaseInfo baseInfo) {
        Object key = baseInfo != null ? baseInfo : getClass();

        msgs.computeIfAbsent(key, k -> new ArrayList<>()).add(msg);

        if (baseInfo != null) {
            if (!infos0().add(baseInfo) && baseInfo instanceof TxEntriesInfo) {
                for (IgniteDiagnosticRequest.DiagnosticBaseInfo baseInfo0 : infos0()) {
                    if (baseInfo0.equals(baseInfo))
                        baseInfo0.merge(baseInfo);
                }
            }
        }
    }

    /** */
    private Collection<DiagnosticBaseInfo> infos0() {
        if (infos == null)
            infos = new LinkedHashSet<>();

        return infos;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /** @return Compound diagnostic infos.  */
    public Collection<DiagnosticBaseInfo> infos() {
        return Collections.unmodifiableCollection(infos0());
    }

    /** */
    public void infos(Collection<DiagnosticBaseInfo> infos) {
        assert infos != null;

        this.infos = new LinkedHashSet<>(infos);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -61;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDiagnosticRequest.class, this);
    }

    /** */
    public abstract static class DiagnosticBaseInfo implements Message {
        /**
         * @param other Another info of the same type.
         */
        public void merge(DiagnosticBaseInfo other) {
            // No-op.
        }

        /**
         * @param sb String builder.
         * @param ctx Grid context.
         */
        public abstract void appendInfo(StringBuilder sb, GridKernalContext ctx);
    }
}
