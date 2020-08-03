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

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * H2 Value message factory.
 */
public class GridH2ValueMessageFactory implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(IgniteMessageFactory factory) {
        factory.register((short)-4, () -> GridH2Null.INSTANCE);
        factory.register((short)-5, GridH2Boolean::new);
        factory.register((short)-6, GridH2Byte::new);
        factory.register((short)-7, GridH2Short::new);
        factory.register((short)-8, GridH2Integer::new);
        factory.register((short)-9, GridH2Long::new);
        factory.register((short)-10, GridH2Decimal::new);
        factory.register((short)-11, GridH2Double::new);
        factory.register((short)-12, GridH2Float::new);
        factory.register((short)-13, GridH2Time::new);
        factory.register((short)-14, GridH2Date::new);
        factory.register((short)-15, GridH2Timestamp::new);
        factory.register((short)-16, GridH2Bytes::new);
        factory.register((short)-17, GridH2String::new);
        factory.register((short)-18, GridH2Array::new);
        factory.register((short)-19, GridH2JavaObject::new);
        factory.register((short)-20, GridH2Uuid::new);
        factory.register((short)-21, GridH2Geometry::new);
        factory.register((short)-22, GridH2CacheObject::new);
        factory.register((short)-30, GridH2IndexRangeRequest::new);
        factory.register((short)-31, GridH2IndexRangeResponse::new);
        factory.register((short)-32, GridH2RowMessage::new);
        factory.register((short)-33, GridH2QueryRequest::new);
        factory.register((short)-34, GridH2RowRange::new);
        factory.register((short)-35, GridH2RowRangeBounds::new);
        factory.register((short)-54, QueryTable::new);
        factory.register((short)-55, GridH2DmlRequest::new);
        factory.register((short)-56, GridH2DmlResponse::new);
        factory.register((short)-57, GridH2SelectForUpdateTxDetails::new);
    }

    /** {@inheritDoc} */
    @Override @Nullable public Message create(short type) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param src Source values.
     * @param dst Destination collection.
     * @param cnt Number of columns to actually send.
     * @return Destination collection.
     * @throws IgniteCheckedException If failed.
     */
    public static Collection<Message> toMessages(Collection<Value[]> src, Collection<Message> dst, int cnt)
        throws IgniteCheckedException {
        for (Value[] row : src) {
            assert row.length >= cnt;

            for (int i = 0; i < cnt; i++)
                dst.add(toMessage(row[i]));
        }

        return dst;
    }

    /**
     * @param src Source iterator.
     * @param dst Array to fill with values.
     * @param ctx Kernal context.
     * @return Filled array.
     * @throws IgniteCheckedException If failed.
     */
    public static Value[] fillArray(Iterator<? extends Message> src, Value[] dst, GridKernalContext ctx)
        throws IgniteCheckedException {
        for (int i = 0; i < dst.length; i++) {
            Message msg = src.next();

            dst[i] = ((GridH2ValueMessage)msg).value(ctx);
        }

        return dst;
    }

    /**
     * @param v Value.
     * @return Message.
     * @throws IgniteCheckedException If failed.
     */
    public static GridH2ValueMessage toMessage(Value v) throws IgniteCheckedException {
        switch (v.getType()) {
            case Value.NULL:
                return GridH2Null.INSTANCE;

            case Value.BOOLEAN:
                return new GridH2Boolean(v);

            case Value.BYTE:
                return new GridH2Byte(v);

            case Value.SHORT:
                return new GridH2Short(v);

            case Value.INT:
                return new GridH2Integer(v);

            case Value.LONG:
                return new GridH2Long(v);

            case Value.DECIMAL:
                return new GridH2Decimal(v);

            case Value.DOUBLE:
                return new GridH2Double(v);

            case Value.FLOAT:
                return new GridH2Float(v);

            case Value.DATE:
                return new GridH2Date(v);

            case Value.TIME:
                return new GridH2Time(v);

            case Value.TIMESTAMP:
                return new GridH2Timestamp(v);

            case Value.BYTES:
                return new GridH2Bytes(v);

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                return new GridH2String(v);

            case Value.ARRAY:
                return new GridH2Array(v);

            case Value.JAVA_OBJECT:
                if (v instanceof GridH2ValueCacheObject)
                    return new GridH2CacheObject((GridH2ValueCacheObject)v);

                return new GridH2JavaObject(v);

            case Value.UUID:
                return new GridH2Uuid(v);

            case Value.GEOMETRY:
                return new GridH2Geometry(v);

            default:
                throw new IllegalStateException("Unsupported H2 type: " + v.getType());
        }
    }
}
