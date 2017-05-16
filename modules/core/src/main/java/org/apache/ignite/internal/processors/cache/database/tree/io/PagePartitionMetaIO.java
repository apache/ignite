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
 *
 */

package org.apache.ignite.internal.processors.cache.database.tree.io;

import org.apache.ignite.internal.pagemem.PageUtils;

/**
 *
 */
public class PagePartitionMetaIO extends PageMetaIO {
    /** */
    private static final int SIZE_OFF = PageMetaIO.END_OF_PAGE_META;

    /** */
    private static final int UPDATE_CNTR_OFF = SIZE_OFF + 8;

    /** */
    private static final int GLOBAL_RMV_ID_OFF = UPDATE_CNTR_OFF + 8;

    /** */
    private static final int PARTITION_STATE_OFF = GLOBAL_RMV_ID_OFF + 8;

    /** */
    public static final IOVersions<PagePartitionMetaIO> VERSIONS = new IOVersions<>(
        new PagePartitionMetaIO(1)
    );

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setSize(pageAddr, 0);
        setUpdateCounter(pageAddr, 0);
        setGlobalRemoveId(pageAddr, 0);
        setPartitionState(pageAddr, (byte)-1);
    }

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIO(int ver) {
        super(T_PART_META, ver);
    }

    /**
     * @param pageAddr Page address.
     * @return Partition size.
     */
    public long getSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, SIZE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param size Partition size.
     */
    public void setSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, SIZE_OFF, size);
    }

    /**
     * @param pageAddr Page address.
     * @return Partition update counter.
     */
    public long getUpdateCounter(long pageAddr) {
        return PageUtils.getLong(pageAddr, UPDATE_CNTR_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param cntr Partition update counter.
     */
    public void setUpdateCounter(long pageAddr, long cntr) {
        PageUtils.putLong(pageAddr, UPDATE_CNTR_OFF, cntr);
    }

    /**
     * @param pageAddr Page address.
     * @return Global remove ID.
     */
    public long getGlobalRemoveId(long pageAddr) {
        return PageUtils.getLong(pageAddr, GLOBAL_RMV_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param rmvId Global remove ID.
     */
    public void setGlobalRemoveId(long pageAddr, long rmvId) {
        PageUtils.putLong(pageAddr, GLOBAL_RMV_ID_OFF, rmvId);
    }

    /**
     * @param pageAddr Page address.
     */
    public byte getPartitionState(long pageAddr) {
        return PageUtils.getByte(pageAddr, PARTITION_STATE_OFF);
    }

    /**
     * @param pageAddr Page address
     * @param state State.
     */
    public void setPartitionState(long pageAddr, byte state) {
        PageUtils.putByte(pageAddr, PARTITION_STATE_OFF, state);
    }
}
