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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.plugin.extensions.communication.MessageConverter;
import org.h2.value.Value;

/**
 * Message converter.
 */
public class GridH2ValueMessageConverter implements MessageConverter<Value, GridH2ValueMessage> {
    /** Singleton instance. */
    public static final GridH2ValueMessageConverter INSTANCE = new GridH2ValueMessageConverter(null);

    /**
     * Create converter for the given context.
     *
     * @param ctx Context.
     * @return Converter.
     */
    public static GridH2ValueMessageConverter forContext(GridKernalContext ctx) {
        return new GridH2ValueMessageConverter(ctx);
    }

    /**
     * Convert value to message.
     *
     * @param val Value.
     * @return Message.
     */
    public static GridH2ValueMessage toMessage(Value val) {
        try {
            switch (val.getType()) {
                case Value.NULL:
                    return GridH2Null.INSTANCE;

                case Value.BOOLEAN:
                    return new GridH2Boolean(val);

                case Value.BYTE:
                    return new GridH2Byte(val);

                case Value.SHORT:
                    return new GridH2Short(val);

                case Value.INT:
                    return new GridH2Integer(val);

                case Value.LONG:
                    return new GridH2Long(val);

                case Value.DECIMAL:
                    return new GridH2Decimal(val);

                case Value.DOUBLE:
                    return new GridH2Double(val);

                case Value.FLOAT:
                    return new GridH2Float(val);

                case Value.DATE:
                    return new GridH2Date(val);

                case Value.TIME:
                    return new GridH2Time(val);

                case Value.TIMESTAMP:
                    return new GridH2Timestamp(val);

                case Value.BYTES:
                    return new GridH2Bytes(val);

                case Value.STRING:
                case Value.STRING_FIXED:
                case Value.STRING_IGNORECASE:
                    return new GridH2String(val);

                case Value.ARRAY:
                    return new GridH2Array(val);

                case Value.JAVA_OBJECT:
                    if (val instanceof GridH2ValueCacheObject)
                        return new GridH2CacheObject((GridH2ValueCacheObject) val);

                    return new GridH2JavaObject(val);

                case Value.UUID:
                    return new GridH2Uuid(val);

                case Value.GEOMETRY:
                    return new GridH2Geometry(val);

                default:
                    throw new IllegalStateException("Unsupported H2 type: " + val.getType());
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to convert H2 value to message: " + val, e);
        }
    }

    /**
     * Convert message to H2 value.
     *
     * @param msg Message.
     * @param ctx Kernal context.
     * @return Value.
     */
    public static Value toValue(GridH2ValueMessage msg, GridKernalContext ctx) {
        try {
            return msg.value(ctx);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to convert message to H2 value: " + msg, e);
        }
    }

    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * Private constructor.
     *
     * @param ctx Kernal context.
     */
    private GridH2ValueMessageConverter(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public GridH2ValueMessage convertOnWrite(Value obj) {
        return toMessage(obj);
    }

    /** {@inheritDoc} */
    @Override public Value convertOnRead(GridH2ValueMessage obj) {
        return toValue(obj, ctx);
    }
}
