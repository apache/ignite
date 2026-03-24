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
        factory.register(() -> GridH2Null.INSTANCE, new GridH2NullSerializer(-4));
        factory.register(GridH2Boolean::new, new GridH2BooleanSerializer(-5));
        factory.register(GridH2Byte::new, new GridH2ByteSerializer(-6));
        factory.register(GridH2Short::new, new GridH2ShortSerializer(-7));
        factory.register(GridH2Integer::new, new GridH2IntegerSerializer(-8));
        factory.register(GridH2Long::new, new GridH2LongSerializer(-9));
        factory.register(GridH2Decimal::new, new GridH2DecimalSerializer(-10));
        factory.register(GridH2Double::new, new GridH2DoubleSerializer(-11));
        factory.register(GridH2Float::new, new GridH2FloatSerializer(-12));
        factory.register(GridH2Time::new, new GridH2TimeSerializer(-13));
        factory.register(GridH2Date::new, new GridH2DateSerializer(-14));
        factory.register(GridH2Timestamp::new, new GridH2TimestampSerializer(-15));
        factory.register(GridH2Bytes::new, new GridH2BytesSerializer(-16));
        factory.register(GridH2String::new, new GridH2StringSerializer(-17));
        factory.register(GridH2Array::new, new GridH2ArraySerializer(-18));
        factory.register(GridH2JavaObject::new, new GridH2JavaObjectSerializer(-19));
        factory.register(GridH2Uuid::new, new GridH2UuidSerializer(-20));
        factory.register(GridH2Geometry::new, new GridH2GeometrySerializer(-21));
        factory.register(GridH2CacheObject::new, new GridH2CacheObjectSerializer(-22));
        factory.register(GridH2IndexRangeRequest::new, new GridH2IndexRangeRequestSerializer(-30));
        factory.register(GridH2IndexRangeResponse::new, new GridH2IndexRangeResponseSerializer(-31));
        factory.register(GridH2RowMessage::new, new GridH2RowMessageSerializer(-32));
        factory.register(GridH2QueryRequest::new, new GridH2QueryRequestSerializer(-33));
        factory.register(GridH2RowRange::new, new GridH2RowRangeSerializer(-34));
        factory.register(GridH2RowRangeBounds::new, new GridH2RowRangeBoundsSerializer(-35));
        factory.register(QueryTable::new, new QueryTableSerializer(-54));
        factory.register(GridH2DmlRequest::new, new GridH2DmlRequestSerializer(-55));
        factory.register(GridH2DmlResponse::new, new GridH2DmlResponseSerializer(-56));
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
