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
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentDescription;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class QueryStartRequest implements MarshalableMessage {
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

    /** */
    @GridDirectTransient
    private Object[] params;

    /** */
    private byte[] paramsBytes;

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public QueryStartRequest(UUID qryId, String schema, String root, AffinityTopologyVersion ver,
        FragmentDescription fragmentDesc, Object[] params) {
        this.schema = schema;
        this.qryId = qryId;
        this.fragmentDesc = fragmentDesc;
        this.ver = ver;
        this.root = root;
        this.params = params;
    }

    /** */
    QueryStartRequest() {}

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
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
     * @return Query parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Object[] parameters() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marshaller) throws IgniteCheckedException {
        if (paramsBytes == null && params != null)
            paramsBytes = marshaller.marshal(params);

        fragmentDesc.prepareMarshal(marshaller);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader ldr) throws IgniteCheckedException {
        if (params == null && paramsBytes != null)
            params = marshaller.unmarshal(paramsBytes, ldr);

        fragmentDesc.prepareUnmarshal(marshaller, ldr);
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

        }

        return reader.afterMessageRead(QueryStartRequest.class);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_START_REQUEST;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }
}
