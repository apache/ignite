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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * IO for partition metadata pages.
 */
public class PagePartitionMetaIOV3 extends PagePartitionMetaIOV2 {
    /** Last reencrypted page index offset. */
    private static final int ENCRYPT_PAGE_IDX_OFF = GAPS_LINK + 8;

    /** Total pages to be reencrypted offset. */
    protected static final int ENCRYPT_PAGE_MAX_OFF = ENCRYPT_PAGE_IDX_OFF + 4;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV3(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize, PageMetrics metrics) {
        super.initNewPage(pageAddr, pageId, pageSize, metrics);

        setEncryptedPageIndex(pageAddr, 0);
        setEncryptedPageCount(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Index of the last reencrypted page.
     */
    @Override public int getEncryptedPageIndex(long pageAddr) {
        return PageUtils.getInt(pageAddr, ENCRYPT_PAGE_IDX_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageIdx Index of the last reencrypted page.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setEncryptedPageIndex(long pageAddr, int pageIdx) {
        if (getEncryptedPageIndex(pageAddr) == pageIdx)
            return false;

        PageUtils.putLong(pageAddr, ENCRYPT_PAGE_IDX_OFF, pageIdx);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @return Total pages to be reencrypted.
     */
    @Override public int getEncryptedPageCount(long pageAddr) {
        return PageUtils.getInt(pageAddr, ENCRYPT_PAGE_MAX_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pagesCnt Total pages to be reencrypted.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setEncryptedPageCount(long pageAddr, int pagesCnt) {
        if (getEncryptedPageCount(pageAddr) == pagesCnt)
            return false;

        PageUtils.putInt(pageAddr, ENCRYPT_PAGE_MAX_OFF, pagesCnt);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void printFields(long pageAddr, GridStringBuilder sb) {
        super.printFields(pageAddr, sb);

        sb.a(",\n\tencryptedPageIndex=").a(getEncryptedPageIndex(pageAddr))
            .a(",\n\tencryptedPageCount=").a(getEncryptedPageCount(pageAddr));
    }

    /**
     * Upgrade page to PagePartitionMetaIOV3.
     *
     * @param pageAddr Page address.
     */
    @Override public void upgradePage(long pageAddr) {
        assert PageIO.getType(pageAddr) == getType();

        int ver = PageIO.getVersion(pageAddr);

        assert ver < getVersion();

        if (ver < 2)
            super.upgradePage(pageAddr);

        PageIO.setVersion(pageAddr, getVersion());

        setEncryptedPageIndex(pageAddr, 0);
        setEncryptedPageCount(pageAddr, 0);
    }
}
