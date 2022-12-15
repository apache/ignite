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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class TransformableBinaryKeyObject extends TransformableBinaryObject implements KeyCacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int part = -1;

    /**
     * Default constructor.
     */
    public TransformableBinaryKeyObject() {
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public TransformableBinaryKeyObject(BinaryObjectEx val, byte[] valBytes) {
        assert val != null || valBytes != null;

        assert !(val instanceof TransformableBinaryObject);

        this.val = val;
        this.valBytes = valBytes;

        if (val != null)
            part = ((KeyCacheObject)val).partition();
    }

    /** {@inheritDoc} */
    @Override protected boolean storeValue(CacheObjectValueContext ctx) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public void partition(int part) {
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject copy(int part) {
        if (this.part == part)
            return this;

        TransformableBinaryKeyObject cp = new TransformableBinaryKeyObject((BinaryObjectEx)val, valBytes);

        cp.partition(part);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        if (!transformed(valBytes))
            return ((CacheObject)val).cacheObjectType();
        else
            return CacheObject.TYPE_BINARY_KEY_TRANSFORMER;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        if (!transformed(valBytes))
            return ((Message)val).directType();
        else
            return 191;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 1:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(KeyCacheObjectImpl.class);
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
            case 1:
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

        }

        return true;
    }
}
