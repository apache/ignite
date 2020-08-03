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

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;

/**
 *
 */
class H2IOUtils {
    /**
     *
     */
    private H2IOUtils() {}

    /**
     * @param row Row.
     * @param pageAddr Page address.
     * @param off Offset.
     * @param storeMvcc {@code True} to store mvcc data.
     */
    static void storeRow(H2CacheRow row, long pageAddr, int off, boolean storeMvcc) {
        assert row.link() != 0;

        PageUtils.putLong(pageAddr, off, row.link());

        if (storeMvcc) {
            long mvccCrdVer = row.mvccCoordinatorVersion();
            long mvccCntr = row.mvccCounter();
            int mvccOpCntr = row.mvccOperationCounter();

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(pageAddr, off + 8, mvccCrdVer);
            PageUtils.putLong(pageAddr, off + 16, mvccCntr);
            PageUtils.putInt(pageAddr, off + 24, mvccOpCntr);
        }
    }

    /**
     * @param dstPageAddr Destination page address.
     * @param dstOff Destination page offset.
     * @param srcIo Source IO.
     * @param srcPageAddr Source page address.
     * @param srcIdx Source index.
     * @param storeMvcc {@code True} to store mvcc data.
     */
    static void store(long dstPageAddr,
        int dstOff,
        BPlusIO<H2Row> srcIo,
        long srcPageAddr,
        int srcIdx,
        boolean storeMvcc)
    {
        H2RowLinkIO rowIo = (H2RowLinkIO)srcIo;

        long link = rowIo.getLink(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, link);

        if (storeMvcc) {
            long mvccCrdVer = rowIo.getMvccCoordinatorVersion(srcPageAddr, srcIdx);
            long mvccCntr = rowIo.getMvccCounter(srcPageAddr, srcIdx);
            int mvccOpCntr = rowIo.getMvccOperationCounter(srcPageAddr, srcIdx);

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(dstPageAddr, dstOff + 8, mvccCrdVer);
            PageUtils.putLong(dstPageAddr, dstOff + 16, mvccCntr);
            PageUtils.putInt(dstPageAddr, dstOff + 24, mvccOpCntr);
        }
    }
}
