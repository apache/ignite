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

package org.apache.ignite.plugin.extensions.communication;

import java.io.*;
import java.nio.*;

/**
 * Base class for all communication messages.
 */
public abstract class MessageAdapter implements Serializable, Cloneable {
    /** Message reader. */
    protected MessageReader reader;

    /** Current read state. */
    protected int readState;

    /**
     * @param reader Message reader.
     */
    public final void setReader(MessageReader reader) {
        assert this.reader == null;
        assert reader != null;

        this.reader = reader;
    }

    /**
     * Writes this message to provided byte buffer.
     *
     * @param buf Byte buffer.
     * @return Whether message was fully written.
     */
    public abstract boolean writeTo(ByteBuffer buf);

    /**
     * Reads this message from provided byte buffer.
     *
     * @param buf Byte buffer.
     * @return Whether message was fully read.
     */
    public abstract boolean readFrom(ByteBuffer buf);

    /**
     * Gets message type.
     *
     * @return Message type.
     */
    public abstract byte directType();

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override public abstract MessageAdapter clone();

    /**
     * Clones all fields of the provided message to this message.
     *
     * @param msg Message to clone from.
     */
    protected abstract void clone0(MessageAdapter msg);

    /**
     * Defines whether recovery for this message should be skipped.
     *
     * @return Whether recovery for this message should be skipped.
     */
    public boolean skipRecovery() {
        return false;
    }
}
