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

import org.apache.ignite.internal.processors.security.SecuritySubjectMessage;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Type of {@link OperationContextAttribute}.
 */
public enum OperationContextAttributeType {
    /** */
    SECURITY(SecuritySubjectMessage.class);

    /** Attribute value type. */
    private final Class<? extends Message> valType;

    /** */
    private OperationContextAttributeType(Class<? extends Message> valType) {
        this.valType = valType;
    }

    /** */
    public static OperationContextAttributeType of(int id) {
        assert id >= 0 && id < values().length;

        return values()[id];
    }

    /** */
    public Class<? extends Message> type() {
        return valType;
    }

    /** */
    public <T extends Message> OperationContextAttribute<T> create(T val) {
        assert val == null || val.getClass().isAssignableFrom(valType);

        return OperationContextAttribute.newInstance(id(), val);
    }

    /** */
    public <T extends Message> @Nullable T get() {
        Object res = OperationContext.get(create(null));

        assert res == null || valType.isAssignableFrom(res.getClass());

        return (T)res;
    }

    /**
     * Attribute id (number). Limited by {@link OperationContextAttribute#MAX_ATTR_CNT}.
     *
     * @see OperationContextAttribute#bitmask()
     */
    public byte id() {
        assert (byte)ordinal() < OperationContextAttribute.MAX_ATTR_CNT;

        return (byte)ordinal();
    }
}
