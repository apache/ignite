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
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
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
    private UUID queryId;

    /** */
    private long fragmentId;

    /** */
    private int[] partitions;

    /** */
    private AffinityTopologyVersion version;

    /** */
    @GridDirectTransient
    private RelGraph plan;

    /** */
    private byte[] planBytes;

    /** */
    @GridDirectTransient
    private Object[] params;

    /** */
    private byte[] paramsBytes;

    /** */
    public QueryStartRequest(UUID queryId, long fragmentId, String schema, RelGraph plan, AffinityTopologyVersion version, int[] partitions, Object[] params) {
        this.schema = schema;
        this.queryId = queryId;
        this.fragmentId = fragmentId;
        this.partitions = partitions;
        this.version = version;
        this.plan = plan;
        this.params = params;
    }

    /** */
    QueryStartRequest() {

    }

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
        return queryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * @return Partitions.
     */
    public int[] partitions() {
        return partitions;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return version;
    }

    /**
     * @return Fragment plan.
     */
    public RelGraph plan() {
        return plan;
    }

    /**
     * @return Query parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marshaller) throws IgniteCheckedException {
        if (planBytes == null && plan != null)
            planBytes = marshaller.marshal(plan);

        if (paramsBytes == null && params != null)
            paramsBytes = marshaller.marshal(params);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader loader) throws IgniteCheckedException {
        if (plan == null && planBytes != null)
            plan = marshaller.unmarshal(planBytes, loader);

        if (params == null && paramsBytes != null)
            params = marshaller.unmarshal(paramsBytes, loader);
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
                if (!writer.writeLong("fragmentId", fragmentId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("paramsBytes", paramsBytes))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIntArray("partitions", partitions))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("planBytes", planBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeUuid("queryId", queryId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeString("schema", schema))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeAffinityTopologyVersion("version", version))
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
                fragmentId = reader.readLong("fragmentId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                paramsBytes = reader.readByteArray("paramsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                partitions = reader.readIntArray("partitions");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                planBytes = reader.readByteArray("planBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                queryId = reader.readUuid("queryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                schema = reader.readString("schema");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                version = reader.readAffinityTopologyVersion("version");

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
