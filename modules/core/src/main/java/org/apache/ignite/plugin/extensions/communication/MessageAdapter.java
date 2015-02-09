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
 * Communication message adapter.
 */
public abstract class MessageAdapter implements Serializable, Cloneable {
    /** Writer. */
    protected MessageWriter writer;

    /** Reader. */
    protected MessageReader reader;

    /** */
    protected boolean typeWritten;

    /** */
    protected int state;

    /**
     * @param writer Writer.
     */
    public final void setWriter(MessageWriter writer) {
        if (this.writer == null)
            this.writer = writer;
    }

    /**
     * @param reader Reader.
     */
    public final void setReader(MessageReader reader) {
        if (this.reader == null)
            this.reader = reader;
    }

    /**
     * @param buf Byte buffer.
     * @return Whether message was fully written.
     */
    public abstract boolean writeTo(ByteBuffer buf);

    /**
     * @param buf Byte buffer.
     * @return Whether message was fully read.
     */
    public abstract boolean readFrom(ByteBuffer buf);

    /**
     * @return Message type.
     */
    public abstract byte directType();

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override public abstract MessageAdapter clone();

    /**
     * Clones all fields of the provided message to {@code this}.
     *
     * @param _msg Message to clone from.
     */
    protected abstract void clone0(MessageAdapter _msg);

    /**
     * @return {@code True} if should skip recovery for this message.
     */
    public boolean skipRecovery() {
        return false;
    }
}
