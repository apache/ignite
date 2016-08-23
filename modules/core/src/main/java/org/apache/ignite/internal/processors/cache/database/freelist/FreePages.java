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

package org.apache.ignite.internal.processors.cache.database.freelist;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 */
public final class FreePages {
    /** */
    private static final int BUCKETS = 256; // Must be power of 2.

    /** */
    private final PageMemory pageMem;

    /** */
    private final int shift;

    /** */
    private final AtomicReferenceArray<long[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final AtomicLongArray bitmap = new AtomicLongArray(BUCKETS >>> 6); //  == BUCKETS / 64

    /**
     * @param pageMem Page memory.
     * @param rootPageId Root page ID for this data structure.
     * @param initNew Need to initialize new data structure in the given page.
     */
    public FreePages(PageMemory pageMem, long rootPageId, boolean initNew) {
        this.pageMem = pageMem;

        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize);
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;

        if (initNew) {
            // TODO
        }
        else {
            // TODO
        }
    }

    private int bucket(int freeSpace) {
        assert freeSpace > 0: freeSpace;

        return freeSpace >>> shift;
    }

    private static int bucketBit(int bucket) {
        return bucket >>> 6; // == bucket / 64
    }

    public long takeDataPage(int freeSpace) {
        int idx = bucket(freeSpace);



        return 0L;
    }

    private int findBucketForTake(int idx) {
        return 0;
    }

    public long takeEmptyPage() {
        return 0L;
    }

    public void putDataPage(long pageId, ByteBuffer buf, int freeSpace) {
        // Do not store pages which are almost full.
        if (freeSpace < 8) {


            return;
        }


    }

    public void putEmptyPage(long pageId) {

    }
}
