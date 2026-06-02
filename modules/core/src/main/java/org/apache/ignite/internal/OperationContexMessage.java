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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Transport for {@link OperationContextSnapshot} attributes.
 *
 * @see OperationContextSnapshot
 */
public class OperationContexMessage implements Message {
    /** Expected maximal number of operation context attributes. Is for message size optimization.  */
    private static final int DFLT_EXPECTED_MAX_ATTRIBUTES_NUMBER = 5;

    /** Effective operation context attributes and their values. */
    @Order(0)
    List<OperationContexAttributeMessage> opCtxAttrs;

    /**
     * Mapping of the attributes: attribute id -> effective attribute index + 1.
     * If attributes index is 0, corresponding attribute is not set.
     */
    @Order(1)
    byte[] attrsMapping = new byte[OperationContextAttribute.MAX_ATTR_CNT];

    /** Empty constructor for serialization purposes. */
    public OperationContexMessage() {
        // No-op.
    }

    /** */
    public boolean hasAttribute(OperationContextAttributeType attrType) {
        assert attrType.id() >= 0 && attrType.id() < attrsMapping.length;

        return attrsMapping[attrType.id()] > 0;
    }

    /** */
    public <T> @Nullable T attributeValue(OperationContextAttributeType attrType) {
        assert attrType.id() >= 0 && attrType.id() < attrsMapping.length;

        byte idx = attrsMapping[attrType.id()];

        if (idx < 1)
            return null;

        --idx;

        assert idx < opCtxAttrs.size();

        OperationContexAttributeMessage attrMsg = opCtxAttrs.get(idx);

        assert attrMsg != null;
        assert attrMsg.type == attrType;

        return (T)attrMsg.val;
    }

    /** */
    public static OperationContexMessage enrich(
        @Nullable OperationContexMessage msg,
        OperationContextAttributeType attrType,
        Message attrVal
    ) {
        if (msg == null) {
            msg = new OperationContexMessage();

            msg.opCtxAttrs = new ArrayList<>(DFLT_EXPECTED_MAX_ATTRIBUTES_NUMBER);
        }

        assert attrType.id() >= 0 && attrType.id() < msg.attrsMapping.length;

        msg.opCtxAttrs.add(new OperationContexAttributeMessage(attrType, attrVal));

        msg.attrsMapping[attrType.id()] = (byte)msg.opCtxAttrs.size();

        return msg;
    }
}
