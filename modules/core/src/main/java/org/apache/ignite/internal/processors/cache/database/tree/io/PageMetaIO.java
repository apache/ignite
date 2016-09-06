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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class PageMetaIO extends PageIO {
    /** Page number offset. */
    private static final int PAGE_NUM_OFF = PageIO.COMMON_HEADER_END;

    /** Metastore root offset. */
    private static final int METASTORE_ROOT_OFF = PAGE_NUM_OFF + 4;

    /** Root ids start offset. */
    private static final int ROOT_IDS_START_OFF = METASTORE_ROOT_OFF + 8;

    /**
     * @param ver Page format version.
     */
    public PageMetaIO(int ver) {
        super(PageIO.T_META, ver);
    }

    /**
     * @param buf Buffer.
     */
    public int getPagesNum(ByteBuffer buf) {
        return buf.getInt(PAGE_NUM_OFF);
    }

    /**
     * @param buf Buffer.
     * @param pageNum Pages number.
     */
    public void setPagesNum(ByteBuffer buf, int pageNum) {
        buf.putInt(PAGE_NUM_OFF, pageNum);
    }

    /**
     * @param buf Buffer.
     */
    public long getMetastoreRoot(ByteBuffer buf) {
        return buf.getLong(METASTORE_ROOT_OFF);
    }

    /**
     * @param buf Buffer.
     * @param metastoreRoot metastore root
     */
    public void setMetastoreRoot(@NotNull ByteBuffer buf, long metastoreRoot) {
        buf.putLong(METASTORE_ROOT_OFF, metastoreRoot);
    }

    /**
     * @param buf Buffer.
     */
    public long[] getRootIds(@NotNull ByteBuffer buf) {
        int pagesNum = getPagesNum(buf);

        if (pagesNum > 0) {

            long[] rootIds = new long[pagesNum - 1];

            for (int i = 0; i < rootIds.length; i++) {
                assert buf.remaining() >= 8 : "Meta page is corrupted [remaining=" + buf.remaining() +
                    ", pagesNum=" + pagesNum + ", i =" + i + ']';

                rootIds[i] = buf.getLong(ROOT_IDS_START_OFF + i * 8);
            }

            return rootIds;
        } else
            return null;
    }

    /**
     * @param buf Buffer.
     * @param rootIds Root ids.
     */
    public void setRootIds(@NotNull ByteBuffer buf, @NotNull long[] rootIds) {
        setPagesNum(buf, rootIds.length + 1);

        for (int i = 0; i < rootIds.length; i++)
            buf.putLong(ROOT_IDS_START_OFF + i * 8, rootIds[i]);
    }
}
