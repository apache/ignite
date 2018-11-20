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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * IO routines for B+Tree meta pages.
 */
public class BPlusMetaIO extends PageIO {
    /** */
    public static final IOVersions<BPlusMetaIO> VERSIONS = new IOVersions<>(
        new BPlusMetaIO(1), new BPlusMetaIO(2)
    );

    /** */
    private static final int LVLS_OFF = COMMON_HEADER_END;

    /** */
    private final int refsOff;

    /** */
    private final int inlineSizeOff;

    /**
     * @param ver Page format version.
     */
    private BPlusMetaIO(int ver) {
        super(T_BPLUS_META, ver);

        switch (ver) {
            case 1:
                inlineSizeOff = -1;
                refsOff = LVLS_OFF + 1;
                break;

            case 2:
                inlineSizeOff = LVLS_OFF + 1;
                refsOff = inlineSizeOff + 2;
                break;

            default:
                throw new IgniteException("invalid IO version: " + ver);
        }
    }

    /**
     * @param pageAdrr Page address.
     * @param rootId Root page ID.
     * @param pageSize Page size.
     */
    public void initRoot(long pageAdrr, long rootId, int pageSize) {
        setLevelsCount(pageAdrr, 1, pageSize);
        setFirstPageId(pageAdrr, 0, rootId);
    }

    /**
     * @param pageAddr Page address.
     * @return Number of levels in this tree.
     */
    public int getLevelsCount(long pageAddr) {
        return Byte.toUnsignedInt(PageUtils.getByte(pageAddr, LVLS_OFF));
    }

    /**
     * @param pageAddr Page address.
     * @param pageSize Page size.
     * @return Max levels possible for this page size.
     */
    private int getMaxLevels(long pageAddr, int pageSize) {
        return (pageSize - refsOff) / 8;
    }

    /**
     * @param pageAddr Page address.
     * @param lvls Number of levels in this tree.
     * @param pageSize Page size.
     */
    private void setLevelsCount(long pageAddr, int lvls, int pageSize) {
        assert lvls >= 0 && lvls <= getMaxLevels(pageAddr, pageSize) : lvls;

        PageUtils.putByte(pageAddr, LVLS_OFF, (byte)lvls);

        assert getLevelsCount(pageAddr) == lvls;
    }

    /**
     * @param lvl Level.
     * @return Offset for page reference.
     */
    private int offset(int lvl) {
        return lvl * 8 + refsOff;
    }

    /**
     * @param pageAddr Page address.
     * @param lvl Level.
     * @return First page ID at that level.
     */
    public long getFirstPageId(long pageAddr, int lvl) {
        return PageUtils.getLong(pageAddr, offset(lvl));
    }

    /**
     * @param pageAddr Page address.
     * @param lvl    Level.
     * @param pageId Page ID.
     */
    private void setFirstPageId(long pageAddr, int lvl, long pageId) {
        assert lvl >= 0 && lvl < getLevelsCount(pageAddr) : lvl;

        PageUtils.putLong(pageAddr, offset(lvl), pageId);

        assert getFirstPageId(pageAddr, lvl) == pageId;
    }

    /**
     * @param pageAddr Page address.
     * @return Root level.
     */
    public int getRootLevel(long pageAddr) {
        int lvls = getLevelsCount(pageAddr); // The highest level page is root.

        assert lvls > 0 : lvls;

        return lvls - 1;
    }

    /**
     * @param pageAddr Page address.
     * @param rootPageId New root page ID.
     * @param pageSize Page size.
     */
    public void addRoot(long pageAddr, long rootPageId, int pageSize) {
        int lvl = getLevelsCount(pageAddr);

        setLevelsCount(pageAddr, lvl + 1, pageSize);
        setFirstPageId(pageAddr, lvl, rootPageId);
    }

    /**
     * @param pageAddr Page address.
     * @param pageSize Page size.
     */
    public void cutRoot(long pageAddr, int pageSize) {
        int lvl = getRootLevel(pageAddr);

        setLevelsCount(pageAddr, lvl, pageSize); // Decrease tree height.
    }

    /**
     * @param pageAddr Page address.
     * @param size Offset size.
     */
    public void setInlineSize(long pageAddr, int size) {
        if (getVersion() > 1)
            PageUtils.putShort(pageAddr, inlineSizeOff, (short)size);
    }

    /**
     * @param pageAddr Page address.
     */
    public int getInlineSize(long pageAddr) {
        return getVersion() > 1 ? PageUtils.getShort(pageAddr, inlineSizeOff) : 0;
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("BPlusMeta [\n\tlevelsCnt=").a(getLevelsCount(addr))
            .a(",\n\trootLvl=").a(getRootLevel(addr))
            .a(",\n\tinlineSize=").a(getInlineSize(addr))
            .a("\n]")
        ;
            //TODO print firstPageIds by level
    }
}
