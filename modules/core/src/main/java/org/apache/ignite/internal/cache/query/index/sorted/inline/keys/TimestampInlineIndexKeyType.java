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

package org.apache.ignite.internal.cache.query.index.sorted.inline.keys;

import java.sql.Timestamp;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link Timestamp} values.
 */
public class TimestampInlineIndexKeyType extends NullableInlineIndexKeyType<Timestamp> {
    /** */
    public TimestampInlineIndexKeyType() {
        super(IndexKeyTypes.TIMESTAMP, (short) 16);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Timestamp v) {
        Timestamp ts = new Timestamp(0);
        ts.setTime(PageUtils.getLong(pageAddr, off + 1));
        ts.setNanos((int)PageUtils.getLong(pageAddr, off + 9));

        long epoch = PageUtils.getLong(pageAddr, off + 1);
        long val2 = ts.getTime();

        int c = Integer.signum(Long.compare(epoch, val2));

        if (c != 0)
            return c;

        long nanos1 = PageUtils.getLong(pageAddr, off + 9);
        long nanos2 = ts.getNanos();

        return Integer.signum(Long.compare(nanos1, nanos2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Timestamp val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());

        PageUtils.putLong(pageAddr, off + 1, val.getTime());
        PageUtils.putLong(pageAddr, off + 9, val.getNanos());

        // TODO: timestamp, H2 DateTimeUtils
//        public static ValueTimestamp get(Timestamp var0) {

//        long epoch_ms = val.getTime();
//        long sec_ms = val.getNanos() % 1000000;

//            long var1 = var0.getTime();
//            long var3 = (long)(var0.getNanos() % 1000000);
//            long var5 = DateTimeUtils.dateValueFromDate(var1);
//            var3 += DateTimeUtils.nanosFromDate(var1);
//            return fromDateValueAndNanos(var5, var3);
//        }

        return keySize() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Timestamp get0(long pageAddr, int off) {
        Timestamp ts = new Timestamp(0);
        ts.setTime(PageUtils.getLong(pageAddr, off + 1));
        ts.setNanos((int)PageUtils.getLong(pageAddr, off + 9));

        return ts;
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Timestamp key) {
        return keySize() + 1;
    }
}
