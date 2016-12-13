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

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.io.WritableUtils;
import org.apache.ignite.internal.processors.hadoop.shuffle.RawOffheapComparator;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Raw offheap comparator for Text.
 */
public class TextRawOffheapComparator implements RawOffheapComparator {
    /** {@inheritDoc} */
    @Override public int compare(byte[] buf1, int off1, int len1, long ptr2, int len2) {
        int len11 = WritableUtils.decodeVIntSize(buf1[off1]);
        int len22 = WritableUtils.decodeVIntSize(GridUnsafe.getByte(ptr2));

        return TextSemiRawOffheapComparator.compareBytes(buf1, off1 + len11, len1 - len11, ptr2 + len22, len2 - len22);
    }
}
