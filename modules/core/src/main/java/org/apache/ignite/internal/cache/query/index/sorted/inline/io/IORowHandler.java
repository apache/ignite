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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Class provide a common logic for storing an index row.
 */
public class IORowHandler {
    /** */
    public static void store(long pageAddr, int off, IndexRow row) {
        // Write link after all inlined idx keys.
        PageUtils.putLong(pageAddr, off, row.link());
    }

    /**
     * @param dstPageAddr Destination page address.
     * @param dstOff Destination page offset.
     * @param srcIo Source IO.
     * @param srcPageAddr Source page address.
     * @param srcIdx Source index.
     */
    static void store(long dstPageAddr, int dstOff, InlineIO srcIo, long srcPageAddr, int srcIdx) {
        long link = srcIo.link(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, link);
    }
}
