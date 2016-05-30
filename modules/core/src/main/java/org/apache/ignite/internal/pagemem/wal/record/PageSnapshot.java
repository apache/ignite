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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.impl.PageMemoryImpl.PAGE_CACHE_ID_OFFSET;
import static org.apache.ignite.internal.pagemem.impl.PageMemoryImpl.PAGE_ID_OFFSET;
import static org.apache.ignite.internal.pagemem.impl.PageMemoryImpl.PAGE_MARKER;
import static org.apache.ignite.internal.pagemem.impl.PageMemoryImpl.PAGE_OVERHEAD;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;

/**
 *
 */
public class PageSnapshot extends PageAbstractRecord {
    /** */
    private byte[] pageData;

    /** */
    private FullPageId fullPageId;

    /**
     * @param arr Backing array.
     */
    public PageSnapshot(byte[] arr) {
        long mark = getLong(arr, BYTE_ARR_OFF);

        if (mark != PAGE_MARKER)
            throw new IllegalArgumentException("Invalid page marker: " + U.hexLong(mark));

        long pageId = getLong(arr, BYTE_ARR_OFF + PAGE_ID_OFFSET);
        int cacheId = getInt(arr, BYTE_ARR_OFF + PAGE_CACHE_ID_OFFSET);

        pageData = new byte[arr.length - PAGE_OVERHEAD];
        copyMemory(arr, BYTE_ARR_OFF + PAGE_OVERHEAD, pageData, BYTE_ARR_OFF, pageData.length);

        fullPageId = new FullPageId(pageId, cacheId);
    }

    /**
     * @return Snapshot of page data.
     */
    public byte[] pageData() {
        return pageData;
    }

    /**
     * @return Full page ID.
     */
    public FullPageId fullPageId() {
        return fullPageId;
    }
}
