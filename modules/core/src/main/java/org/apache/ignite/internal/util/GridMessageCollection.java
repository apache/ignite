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

package org.apache.ignite.internal.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Collection of messages.
 */
public final class GridMessageCollection<M extends Message> implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectCollection(Message.class)
    private Collection<M> msgs;

    /**
     *
     */
    public GridMessageCollection() {
        // No-op.
    }

    /**
     * @param msgs Collection of messages.
     */
    public GridMessageCollection(Collection<M> msgs) {
        this.msgs = msgs;
    }

    /**
     * @param msgs Messages.
     * @return Message list.
     */
    public static <X extends Message> GridMessageCollection<X> of(X... msgs) {
        if (msgs == null || msgs.length == 0)
            return null;

        List<X> list = msgs.length == 1 ? Collections.singletonList(msgs[0]) : Arrays.asList(msgs);

        return new GridMessageCollection<>(list);
    }

    /**
     * @return Messages.
     */
    public Collection<M> messages() {
        return msgs;
    }

    /**
     * @param msgs Messages.
     */
    public void messages(Collection<M> msgs) {
        this.msgs = msgs;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
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
                if (!writer.writeCollection("msgs", msgs, MessageCollectionItemType.MSG))
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
                msgs = reader.readCollection("msgs", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridMessageCollection.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 124;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridMessageCollection<?> that = (GridMessageCollection<?>)o;

        return msgs == that.msgs || (msgs != null && msgs.equals(that.msgs));
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return msgs != null ? msgs.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMessageCollection.class, this, "msgsSize", msgs == null ? null : msgs.size());
    }
}
