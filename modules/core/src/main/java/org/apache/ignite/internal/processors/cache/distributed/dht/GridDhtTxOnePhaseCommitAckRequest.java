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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * One Phase Commit Near transaction ack request.
 */
public class GridDhtTxOnePhaseCommitAckRequest extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock or transaction versions. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    protected Collection<GridCacheVersion> vers;

    /**
     * Default constructor.
     */
    public GridDhtTxOnePhaseCommitAckRequest() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }

    /**
     *
     * @param vers Near Tx xid Versions.
     */
    public GridDhtTxOnePhaseCommitAckRequest(Collection<GridCacheVersion> vers) {
        this.vers = vers;
    }

    /**
     * @return Version.
     */
    public Collection<GridCacheVersion> versions() {
        return vers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxOnePhaseCommitAckRequest.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeCollection("vers", vers, MessageCollectionItemType.MSG))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                vers = reader.readCollection("vers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxOnePhaseCommitAckRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -27;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }
}
