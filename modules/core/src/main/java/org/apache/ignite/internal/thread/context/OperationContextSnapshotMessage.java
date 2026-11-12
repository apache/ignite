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

package org.apache.ignite.internal.thread.context;

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.AbstractMessage;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Message for {@link OperationContext} distributed attributes.
 *
 * @see DistributedOperationContext
 */
public class OperationContextSnapshotMessage extends AbstractMessage {
    /** Maximal number of supported distributed attributes. */
    static final int MAX_ATTRS_CNT = Byte.SIZE;

    /** Values of operation context attributes, in ascending id order. */
    @Order(0)
    Message[] attrs;

    /** Bitmap of attributes ids. */
    @Order(1)
    byte idBitmap;

    /** Empty constructor for serialization purposes. */
    public OperationContextSnapshotMessage() {
        // No-op.
    }

    /** */
    OperationContextSnapshotMessage(byte idBitmap, Message[] attrs) {
        assert attrs.length == attributesCount(idBitmap);

        this.attrs = attrs;
        this.idBitmap = idBitmap;
    }

    /** */
    Message getValue(int attrId) {
        assert contains(idBitmap, attrId);

        return attrs[valueIndex(attrId)];
    }

    /** */
    private int valueIndex(int attrId) {
        return Integer.bitCount(idBitmap & ((1 << attrId) - 1));
    }

    /** */
    static int attributesCount(byte idBitmap) {
        return Integer.bitCount(Byte.toUnsignedInt(idBitmap));
    }

    /** */
    static int nextAttributeId(byte idBitmap, int prevId) {
        int bits = Byte.toUnsignedInt(idBitmap) & (-1 << (prevId + 1));

        return bits == 0 ? -1 : Integer.numberOfTrailingZeros(bits);
    }

    /** */
    static boolean contains(byte idBitmap, int attrId) {
        return (idBitmap & (1 << attrId)) != 0;
    }

    /** */
    static byte set(byte idBitmap, int attrId) {
        return (byte)(idBitmap | (1 << attrId));
    }
}
