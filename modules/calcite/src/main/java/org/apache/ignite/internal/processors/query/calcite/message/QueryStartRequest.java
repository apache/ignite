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
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
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
    @GridDirectCollection(QueryTxEntry.class)
    private @Nullable Collection<QueryTxEntry> qryTxEntries;

    /** */
    @GridDirectMap(keyType = String.class, valueType = String.class)
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
        Collection<QueryTxEntry> qryTxEntries,
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
        this.qryTxEntries = qryTxEntries;
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

    /**
     * @return Transaction entries to mixin on query processing.
     */
    public @Nullable Collection<QueryTxEntry> queryTransactionEntries() {
        return qryTxEntries;
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

        if (qryTxEntries != null) {
            for (QueryTxEntry e : qryTxEntries)
                e.prepareMarshal(ctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        ClassLoader ldr = U.resolveClassLoader(ctx.gridConfig());

        if (params == null && paramsBytes != null)
            params = U.unmarshal(ctx, paramsBytes, ldr);

        fragmentDesc.prepareUnmarshal(ctx);

        if (qryTxEntries != null) {
            for (QueryTxEntry e : qryTxEntries)
                e.prepareUnmarshal(ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage(fragmentDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong(originatingQryId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray(paramsBytes))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeUuid(qryId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeString(root))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeString(schema))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong(timeout))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt(totalFragmentsCnt))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(qryTxEntries, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeAffinityTopologyVersion(ver))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(appAttrs, MessageCollectionItemType.STRING, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                fragmentDesc = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                originatingQryId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                paramsBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qryId = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                root = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                schema = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                timeout = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                totalFragmentsCnt = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                qryTxEntries = reader.readCollection(MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                ver = reader.readAffinityTopologyVersion();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                appAttrs = reader.readMap(MessageCollectionItemType.STRING, MessageCollectionItemType.STRING, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_START_REQUEST;
    }
}
