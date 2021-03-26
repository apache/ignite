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

package org.apache.ignite.table.impl;

import java.nio.ByteBuffer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.table.RowChunk;
import org.apache.ignite.internal.table.TableRow;

/**
 * Dummy implementation class.
 */
public class TestTableRowImpl implements TableRow {
    /** Key offset in tuple. */
    private static final int KEY_OFFSET = Row.KEY_HASH_FIELD_OFFSET;

    /** Payload. */
    private final byte[] bytes;

    /**
     * Constructor.
     *
     * @param bytes Bytes to wrap.
     */
    public TestTableRowImpl(byte[] bytes) {
        this.bytes = bytes.clone();
    }

    /** {@inheritDoc} */
    @Override public byte[] toBytes() {
        return bytes.clone();
    }

    /** {@inheritDoc} */
    @Override public RowChunk keyChunk() {
        return new TestTableRowImpl(bytes) {
            @Override public byte[] toBytes() {
                ByteBuffer buf = ByteBuffer.wrap(bytes());

                int keyLen = buf.getInt(KEY_OFFSET);

                return buf.position(KEY_OFFSET).limit(keyLen).slice().array();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public RowChunk valueChunk() {
        return new TestTableRowImpl(bytes) {
            @Override public byte[] toBytes() {
                ByteBuffer buf = ByteBuffer.wrap(bytes());

                int valOffset = KEY_OFFSET + buf.getInt(KEY_OFFSET);

                return buf.position(valOffset).slice().array();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String colName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectField(String colName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String colName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String colName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int intValue(String colName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long longValue(String colName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String colName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String colName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String colName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long schemaVersion() {
        return 0;
    }

    /**
     *
     */
    private byte[] bytes() {
        return bytes;
    }
}
