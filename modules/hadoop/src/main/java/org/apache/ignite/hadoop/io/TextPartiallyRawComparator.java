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

package org.apache.ignite.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils;
import org.apache.ignite.internal.processors.hadoop.io.OffheapRawMemory;
import org.apache.ignite.internal.processors.hadoop.io.PartiallyOffheapRawComparatorEx;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Partial raw comparator for {@link Text} data type.
 * <p>
 * Implementation is borrowed from {@code org.apache.hadoop.io.FastByteComparisons} and adopted to Ignite
 * infrastructure.
 */
public class TextPartiallyRawComparator implements PartiallyRawComparator<Text>, PartiallyOffheapRawComparatorEx<Text> {
    /** {@inheritDoc} */
    @Override public int compare(Text val1, RawMemory val2Buf) {
        if (val2Buf instanceof OffheapRawMemory) {
            OffheapRawMemory val2Buf0 = (OffheapRawMemory)val2Buf;

            return compare(val1, val2Buf0.pointer(), val2Buf0.length());
        }
        else
            throw new UnsupportedOperationException("Text can be compared only with offheap memory.");
    }

    /** {@inheritDoc} */
    @Override public int compare(Text val1, long val2Ptr, int val2Len) {
        int len2 = WritableUtils.decodeVIntSize(GridUnsafe.getByte(val2Ptr));

        return HadoopUtils.compareBytes(val1.getBytes(), val1.getLength(), val2Ptr + len2, val2Len - len2);
    }
}
