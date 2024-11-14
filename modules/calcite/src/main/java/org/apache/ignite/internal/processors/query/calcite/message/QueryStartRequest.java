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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class QueryStartRequest implements MarshalableMessage, ExecutionContextAware {
    /** */
    private String schema;

    /** */
    private UUID qryId;

    /** */
    private long originatingQryId;

    /** */
    private AffinityTopologyVersion ver;

    /** */
    private FragmentDescription fragmentDesc;

    /** */
    private String root;

    /** Total count of fragments in query for this node. */
    private int totalFragmentsCnt;

    /** */
    @GridDirectTransient
    private Object[] params;

    /** */
    private byte[] paramsBytes;

    /** */
    private long timeout;

    /** */
    private Map<String, String> appAttrs;

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public QueryStartRequest(
        UUID qryId,
        long originatingQryId,
        String schema,
        String root,
        AffinityTopologyVersion ver,
        FragmentDescription fragmentDesc,
        int totalFragmentsCnt,
        Object[] params,
        @Nullable byte[] paramsBytes,
        long timeout,
        @Nullable Map<String, String> appAttrs
    ) {
        this.qryId = qryId;
        this.originatingQryId = originatingQryId;
        this.schema = schema;
        this.root = root;
        this.ver = ver;
        this.fragmentDesc = fragmentDesc;
        this.totalFragmentsCnt = totalFragmentsCnt;
        this.params = params;
        this.paramsBytes = paramsBytes; // If we already have marshalled params, use it.
        this.timeout = timeout;
        this.appAttrs = appAttrs;
    }

    /** */
    QueryStartRequest() {}

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public UUID queryId() {
        return qryId;
    }

    /**
     * @return Registered local query ID on originating node.
     */
    public long originatingQryId() {
        return originatingQryId;
    }

    /** {@inheritDoc} */
    @Override public long fragmentId() {
        return fragmentDesc.fragmentId();
    }

    /**
     * @return Fragment description.
     */
    public FragmentDescription fragmentDescription() {
        return fragmentDesc;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return ver;
    }

    /**
     * @return Fragment plan.
     */
    public String root() {
        return root;
    }

    /**
     * @return Total count of fragments in query for this node.
     */
    public int totalFragmentsCount() {
        return totalFragmentsCnt;
    }

    /**
     * @return Query parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Object[] parameters() {
        return params;
    }

    /**
     * @return Query parameters marshalled.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public byte[] parametersMarshalled() {
        return paramsBytes;
    }

    /**
     * @return Query timeout.
     */
    public long timeout() {
        return timeout;
    }

    /** */
    public Map<String, String> appAttrs() {
        return appAttrs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (paramsBytes == null && params != null)
            paramsBytes = U.marshal(ctx, params);

        fragmentDesc.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (params == null && paramsBytes != null)
            params = U.unmarshal(ctx, paramsBytes, U.resolveClassLoader(ctx.gridConfig()));

        fragmentDesc.prepareUnmarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage("fragmentDesc", fragmentDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("originatingQryId", originatingQryId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray("paramsBytes", paramsBytes))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeUuid("qryId", qryId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeString("root", root))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeString("schema", schema))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("totalFragmentsCnt", totalFragmentsCnt))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeAffinityTopologyVersion("ver", ver))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap("appAttrs", appAttrs, MessageCollectionItemType.STRING, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                fragmentDesc = reader.readMessage("fragmentDesc");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                originatingQryId = reader.readLong("originatingQryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                paramsBytes = reader.readByteArray("paramsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qryId = reader.readUuid("qryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                root = reader.readString("root");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                schema = reader.readString("schema");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                totalFragmentsCnt = reader.readInt("totalFragmentsCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                ver = reader.readAffinityTopologyVersion("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                appAttrs = reader.readMap("appAttrs", MessageCollectionItemType.STRING, MessageCollectionItemType.STRING, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(QueryStartRequest.class);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_START_REQUEST;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }
}
