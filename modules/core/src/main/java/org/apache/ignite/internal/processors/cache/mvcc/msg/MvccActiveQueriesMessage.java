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

package org.apache.ignite.internal.processors.cache.mvcc.msg;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MvccActiveQueriesMessage implements MvccMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridLongList activeQrys;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public MvccActiveQueriesMessage() {
        // No-op.
    }

    /**
     * @param activeQrys Active queries.
     */
    public MvccActiveQueriesMessage(GridLongList activeQrys) {
        this.activeQrys = activeQrys;
    }

    /**
     * @return Active queries.
     */
    @Nullable public GridLongList activeQueries() {
        return activeQrys;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean processedFromNioThread() {
        return true;
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
                if (!writer.writeMessage("activeQrys", activeQrys))
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
                activeQrys = reader.readMessage("activeQrys");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccActiveQueriesMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 149;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccActiveQueriesMessage.class, this);
    }
}
