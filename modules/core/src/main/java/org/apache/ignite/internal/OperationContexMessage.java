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

package org.apache.ignite.internal;

import java.util.Map;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** ransport for {@link OperationContext} attributes. */
public class OperationContexMessage implements Message {
    /** Values of operation context attributes. */
    @Order(0)
    public Message[] vals;

    /** Bitmask of effective attributes ids. */
    @Order(1)
    public byte idBitmask;

    /** Empty constructor for serialization purposes. */
    public OperationContexMessage() {
        // No-op.
    }

    /** */
    public static @Nullable OperationContexMessage create(Map<Byte, Message> attrs) {
        if (F.isEmpty(attrs))
            return null;

        OperationContexMessage res = new OperationContexMessage();

        res.vals = new Message[attrs.size()];

        int idx = 0;

        for (Map.Entry<Byte, Message> e : attrs.entrySet()) {
            byte attrId = e.getKey();
            Message msgVal = e.getValue();

            assert attrId >= 0 && attrId < Byte.SIZE;

            byte mask = (byte)(1 << attrId);

            assert (res.idBitmask & mask) == 0;

            res.idBitmask |= mask;
            res.vals[idx++] = msgVal;
        }

        return res;
    }
}
