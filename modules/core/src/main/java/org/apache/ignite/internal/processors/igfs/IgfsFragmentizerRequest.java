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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Fragmentizer request. Sent from coordinator to other IGFS nodes when colocated part of file
 * should be fragmented.
 */
public class IgfsFragmentizerRequest extends IgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private IgniteUuid fileId;

    /** Ranges to fragment. */
    @GridToStringInclude
    @GridDirectCollection(IgfsFileAffinityRange.class)
    private Collection<IgfsFileAffinityRange> fragmentRanges;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsFragmentizerRequest() {
        // No-op.
    }

    /**
     * @param fileId File id to fragment.
     * @param fragmentRanges Ranges to fragment.
     */
    public IgfsFragmentizerRequest(IgniteUuid fileId, Collection<IgfsFileAffinityRange> fragmentRanges) {
        this.fileId = fileId;
        this.fragmentRanges = fragmentRanges;
    }

    /**
     * @return File ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Fragment ranges.
     */
    public Collection<IgfsFileAffinityRange> fragmentRanges() {
        return fragmentRanges;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsFragmentizerRequest.class, this);
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
            case 0:
                if (!writer.writeIgniteUuid("fileId", fileId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection("fragmentRanges", fragmentRanges, MessageCollectionItemType.MSG))
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
            case 0:
                fileId = reader.readIgniteUuid("fileId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                fragmentRanges = reader.readCollection("fragmentRanges", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgfsFragmentizerRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 69;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }
}