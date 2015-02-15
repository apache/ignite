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

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

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
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        IgfsFragmentizerRequest _clone = (IgfsFragmentizerRequest)_msg;

        _clone.fileId = fileId;
        _clone.fragmentRanges = fragmentRanges;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeIgniteUuid("fileId", fileId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection("fragmentRanges", fragmentRanges, Type.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 0:
                fileId = reader.readIgniteUuid("fileId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                fragmentRanges = reader.readCollection("fragmentRanges", Type.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 69;
    }
}
