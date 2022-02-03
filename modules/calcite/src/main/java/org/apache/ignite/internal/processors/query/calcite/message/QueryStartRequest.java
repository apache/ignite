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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
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
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public QueryStartRequest(
        UUID qryId,
        String schema,
        String root,
        AffinityTopologyVersion ver,
        FragmentDescription fragmentDesc,
        int totalFragmentsCnt,
        Object[] params,
        @Nullable byte[] paramsBytes
    ) {
        this.qryId = qryId;
        this.schema = schema;
        this.root = root;
        this.ver = ver;
        this.fragmentDesc = fragmentDesc;
        this.totalFragmentsCnt = totalFragmentsCnt;
        this.params = params;
        this.paramsBytes = paramsBytes; // If we already have marshalled params, use it.
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

    /** {@inheritDoc} */
    @Override public void prepareMarshal(MarshallingContext ctx) throws IgniteCheckedException {
        if (paramsBytes == null && params != null)
            paramsBytes = ctx.marshal(params);

        fragmentDesc.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(MarshallingContext ctx) throws IgniteCheckedException {
        if (params == null && paramsBytes != null)
            params = ctx.unmarshal(paramsBytes);

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
                if (!writer.writeMessage("fragmentDescription", fragmentDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("paramsBytes", paramsBytes))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("queryId", qryId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeString("root", root))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeString("schema", schema))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeAffinityTopologyVersion("version", ver))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt("totalFragmentsCnt", totalFragmentsCnt))
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
                fragmentDesc = reader.readMessage("fragmentDescription");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                paramsBytes = reader.readByteArray("paramsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                qryId = reader.readUuid("queryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                root = reader.readString("root");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                schema = reader.readString("schema");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                ver = reader.readAffinityTopologyVersion("version");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                totalFragmentsCnt = reader.readInt("totalFragmentsCnt");

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
        return 7;
    }
}
