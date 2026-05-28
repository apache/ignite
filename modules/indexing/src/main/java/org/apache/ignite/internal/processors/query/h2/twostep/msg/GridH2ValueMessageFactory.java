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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.QueryTable;
import org.apache.ignite.internal.processors.query.h2.QueryTableSerializer;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.h2.value.Value;

/**
 * H2 Value message factory.
 */
public class GridH2ValueMessageFactory implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        factory.register(-4, () -> GridH2Null.INSTANCE, new GridH2NullSerializer(U.gridClassLoader()));
        factory.register(-5, GridH2Boolean::new, new GridH2BooleanSerializer(U.gridClassLoader()));
        factory.register(-6, GridH2Byte::new, new GridH2ByteSerializer(U.gridClassLoader()));
        factory.register(-7, GridH2Short::new, new GridH2ShortSerializer(U.gridClassLoader()));
        factory.register(-8, GridH2Integer::new, new GridH2IntegerSerializer(U.gridClassLoader()));
        factory.register(-9, GridH2Long::new, new GridH2LongSerializer(U.gridClassLoader()));
        factory.register(-10, GridH2Decimal::new, new GridH2DecimalSerializer(U.gridClassLoader()));
        factory.register(-11, GridH2Double::new, new GridH2DoubleSerializer(U.gridClassLoader()));
        factory.register(-12, GridH2Float::new, new GridH2FloatSerializer(U.gridClassLoader()));
        factory.register(-13, GridH2Time::new, new GridH2TimeSerializer(U.gridClassLoader()));
        factory.register(-14, GridH2Date::new, new GridH2DateSerializer(U.gridClassLoader()));
        factory.register(-15, GridH2Timestamp::new, new GridH2TimestampSerializer(U.gridClassLoader()));
        factory.register(-16, GridH2Bytes::new, new GridH2BytesSerializer(U.gridClassLoader()));
        factory.register(-17, GridH2String::new, new GridH2StringSerializer(U.gridClassLoader()));
        factory.register(-18, GridH2Array::new, new GridH2ArraySerializer(U.gridClassLoader()));
        factory.register(-19, GridH2JavaObject::new, new GridH2JavaObjectSerializer(U.gridClassLoader()));
        factory.register(-20, GridH2Uuid::new, new GridH2UuidSerializer(U.gridClassLoader()));
        factory.register(-21, GridH2Geometry::new, new GridH2GeometrySerializer(U.gridClassLoader()));
        factory.register(-22, GridH2CacheObject::new, new GridH2CacheObjectSerializer(U.gridClassLoader()));
        factory.register(-30, GridH2IndexRangeRequest::new, new GridH2IndexRangeRequestSerializer(U.gridClassLoader()));
        factory.register(-31, GridH2IndexRangeResponse::new, new GridH2IndexRangeResponseSerializer(U.gridClassLoader()));
        factory.register(-32, GridH2RowMessage::new, new GridH2RowMessageSerializer(U.gridClassLoader()));
        factory.register(-33, GridH2QueryRequest::new, new GridH2QueryRequestSerializer(U.gridClassLoader()));
        factory.register(-34, GridH2RowRange::new, new GridH2RowRangeSerializer(U.gridClassLoader()));
        factory.register(-35, GridH2RowRangeBounds::new, new GridH2RowRangeBoundsSerializer(U.gridClassLoader()));
        factory.register(-54, QueryTable::new, new QueryTableSerializer(U.gridClassLoader()));
        factory.register(-55, GridH2DmlRequest::new, new GridH2DmlRequestSerializer(U.gridClassLoader()));
        factory.register(-56, GridH2DmlResponse::new, new GridH2DmlResponseSerializer(U.gridClassLoader()));
    }

    /**
     * @param src Source values.
     * @param cnt Number of columns to actually send.
     * @return Destination collection.
     * @throws IgniteCheckedException If failed.
     */
    public static Collection<Message> toMessages(Collection<Value[]> src, int cnt)
        throws IgniteCheckedException {
        List<Message> dst = new ArrayList<>(src.size() * cnt);

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
