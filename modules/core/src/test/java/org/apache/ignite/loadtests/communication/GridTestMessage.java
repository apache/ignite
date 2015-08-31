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

package org.apache.ignite.loadtests.communication;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
class GridTestMessage implements Message, Externalizable {
    /** */
    private IgniteUuid id;

    /** */
    private long field1;

    /** */
    private long field2;

    /** */
    private String str;

    /** */
    private byte[] bytes;

    /**
     * @param id Message ID.
     * @param str String.
     */
    GridTestMessage(IgniteUuid id, String str) {
        this.id = id;
        this.str = str;
    }

    /**
     * @param id Message ID.
     * @param bytes Bytes.
     */
    GridTestMessage(IgniteUuid id, byte[] bytes) {
        this.id = id;
        this.bytes = bytes;
    }

    /**
     * For Externalizable support.
     */
    public GridTestMessage() {
        // No-op.
    }

    /**
     * @return Message ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @return Bytes.
     */
    public byte[] bytes() {
        return bytes;
    }

    /**
     * @param bytes Bytes.
     */
    public void bytes(byte[] bytes) {
        this.bytes = bytes;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        out.writeLong(field1);
        out.writeLong(field2);
        U.writeString(out, str);
        U.writeByteArray(out, bytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        field1 = in.readLong();
        field2 = in.readLong();
        str = U.readString(in);
        bytes = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }
}