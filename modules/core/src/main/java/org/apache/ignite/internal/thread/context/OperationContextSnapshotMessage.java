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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.MAX_ATTRS_CNT;

/**
 * Message for {@link OperationContext} distributed attributes.
 *
 * @see OperationContextDispatcher
 */
public class OperationContextSnapshotMessage implements Message {
    /** Values of operation context attributes. */
    @Order(0)
    Message[] attrs;

    /** Bitmap of effective attributes ids. */
    @Order(1)
    byte idBitmap;

    /** Empty constructor for serialization purposes. */
    public OperationContextSnapshotMessage() {
        // No-op.
    }

    /** */
    private OperationContextSnapshotMessage(byte idBitmap, Message[] attrs) {
        this.attrs = attrs;
        this.idBitmap = idBitmap;
    }

    /** */
    public static class Builder {
        /** */
        private byte bitmap = 0;

        /** */
        private List<Message> vals;

        /** */
        public void add(int attrId, Message attrVal) {
            if (vals == null)
                vals = new ArrayList<>(MAX_ATTRS_CNT / 2);

            byte mask = (byte)(1 << attrId);

            assert (bitmap & mask) == 0;

            vals.add(attrVal);
            bitmap |= mask;
        }

        /** */
        public boolean isEmpty() {
            return bitmap == 0;
        }

        /** */
        OperationContextSnapshotMessage build() {
            return new OperationContextSnapshotMessage(bitmap, vals.toArray(Message[]::new));
        }

        /** */
        public static OperationContextSnapshotMessage.Builder create() {
            return new Builder();
        }
    }
}
