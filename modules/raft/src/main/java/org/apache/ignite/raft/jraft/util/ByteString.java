/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

// TODO asch readResolve for empty string. Get rid and use utility class class ByteArray ? IGNITE-14832
public class ByteString implements Externalizable {
    public static final ByteString EMPTY = new ByteString(ByteBuffer.wrap(new byte[0]));

    private ByteBuffer buf;

    public ByteString() {
        // Externalizable.
    }

    public ByteString(ByteBuffer buf) {
        this.buf = buf;
    }

    public ByteString(byte[] bytes) {
        this.buf = ByteBuffer.wrap(bytes);
    }

    public int size() {
        return buf.capacity();
    }

    public ByteBuffer asReadOnlyByteBuffer() {
        return buf.asReadOnlyBuffer();
    }

    public byte byteAt(int pos) {
        return buf.get(pos);
    }

    public void writeTo(OutputStream outputStream) throws IOException {
        WritableByteChannel channel = Channels.newChannel(outputStream);

        channel.write(buf);
    }

    public byte[] toByteArray() {
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        buf.flip();
        return arr;
    }

    public ByteString copy() {
        return this == EMPTY ? EMPTY : new ByteString(toByteArray());
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ByteString that = (ByteString) o;

        return buf.equals(that.buf);
    }

    @Override public int hashCode() {
        return buf.hashCode();
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        byte[] bytes = toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);

        buf = ByteBuffer.wrap(data);
    }
}
