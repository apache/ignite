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
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * IO routines for B+Tree meta pages.
 */
public class BPlusMetaIO extends PageIO {
    /** */
    public static final IOVersions<BPlusMetaIO> VERSIONS = new IOVersions<>(
        new BPlusMetaIO(1),
        new BPlusMetaIO(2),

        // Unwrapped PK & inline POJO
        new BPlusMetaIO(3),

        // Add feature flags.
        new BPlusMetaIO(4)
    );

    /** */
    private static final int LVLS_OFFSET = COMMON_HEADER_END;

    /** */
    private static final int INLINE_SIZE_OFFSET = LVLS_OFFSET + 1;

    /** */
    private static final int FLAGS_OFFSET = INLINE_SIZE_OFFSET + 2;

    /** */
    private static final int CREATED_VER_OFFSET = FLAGS_OFFSET + 8;

    /** */
    private static final int REFS_OFFSET = CREATED_VER_OFFSET + IgniteProductVersion.SIZE_IN_BYTES;

    /** */
    private static final long FLAG_UNWRAPPED_PK = 1L;

    /** */
    private static final long FLAG_INLINE_OBJECT_SUPPORTED = 2L;

    /** */
    private static final long FLAG_INLINE_OBJECT_HASH = 4L;

    /** */
    public static final long DEFAULT_FLAGS = FLAG_UNWRAPPED_PK | FLAG_INLINE_OBJECT_SUPPORTED | FLAG_INLINE_OBJECT_HASH;

    /** */
    private final int refsOff;

    /**
     * @param ver Page format version.
     */
    private BPlusMetaIO(int ver) {
        super(T_BPLUS_META, ver);

        switch (ver) {
            case 1:
                refsOff = LVLS_OFFSET + 1;
                break;

            case 2:
                refsOff = INLINE_SIZE_OFFSET + 2;
                break;

            case 3:
                refsOff = INLINE_SIZE_OFFSET + 2;
                break;

            case 4:
                refsOff = REFS_OFFSET;
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
        return Byte.toUnsignedInt(PageUtils.getByte(pageAddr, LVLS_OFFSET));
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

        PageUtils.putByte(pageAddr, LVLS_OFFSET, (byte)lvls);

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
            PageUtils.putShort(pageAddr, INLINE_SIZE_OFFSET, (short)size);
    }

    /**
     * @param pageAddr Page address.
     * @return Inline size.
     */
    public int getInlineSize(long pageAddr) {
        return getVersion() > 1 ? PageUtils.getShort(pageAddr, INLINE_SIZE_OFFSET) : 0;
    }

    /**
     * @return {@code true} In case use unwrapped PK.
     */
    public boolean unwrappedPk(long pageAddr) {
        return supportFlags() && (flags(pageAddr) & FLAG_UNWRAPPED_PK) != 0L || getVersion() == 3;
    }

    /**
     * @param pageAddr Page address.
     * @return {@code true} In case inline object is supported by the tree.
     */
    public boolean inlineObjectSupported(long pageAddr) {
        assert supportFlags();

        return (flags(pageAddr) & FLAG_INLINE_OBJECT_SUPPORTED) != 0L;
    }

    /**
     * Whether Java objects should be inlined as hash or as bytes array.
     *
     * @param pageAddr Page address.
     */
    public boolean inlineObjectHash(long pageAddr) {
        assert supportFlags();

        return (flags(pageAddr) & FLAG_INLINE_OBJECT_HASH) != 0L;
    }

    /**
     * @return {@code true} If flags are supported.
     */
    public boolean supportFlags() {
        return getVersion() > 3;
    }

    /**
     * @param pageAddr Page address.
     * @param flags Flags.
     * @param createdVer The version of the product that creates the page (b+tree).
     */
    public void initFlagsAndVersion(long pageAddr, long flags, IgniteProductVersion createdVer) {
        PageUtils.putLong(pageAddr, FLAGS_OFFSET, flags);

        setCreatedVersion(pageAddr, createdVer);
    }

    /**
     * @param pageAddr Page address.
     * @param curVer Ignite current version.
     */
    public void setCreatedVersion(long pageAddr, IgniteProductVersion curVer) {
        assert curVer != null;

        PageUtils.putByte(pageAddr, CREATED_VER_OFFSET, curVer.major());
        PageUtils.putByte(pageAddr, CREATED_VER_OFFSET + 1, curVer.minor());
        PageUtils.putByte(pageAddr, CREATED_VER_OFFSET + 2, curVer.maintenance());
        PageUtils.putLong(pageAddr, CREATED_VER_OFFSET + 3, curVer.revisionTimestamp());
        PageUtils.putBytes(pageAddr, CREATED_VER_OFFSET + 11, curVer.revisionHash());
    }

    /**
     * @param pageAddr Page address.
     * @return The version of product that creates the page.
     */
    public IgniteProductVersion createdVersion(long pageAddr) {
        if (getVersion() < 4)
            return null;

        return new IgniteProductVersion(
            PageUtils.getByte(pageAddr, CREATED_VER_OFFSET),
            PageUtils.getByte(pageAddr, CREATED_VER_OFFSET + 1),
            PageUtils.getByte(pageAddr, CREATED_VER_OFFSET + 2),
            PageUtils.getLong(pageAddr, CREATED_VER_OFFSET + 3),
            PageUtils.getBytes(pageAddr, CREATED_VER_OFFSET + 11, IgniteProductVersion.REV_HASH_SIZE));
    }

    /**
     * @param pageAddr Page address.
     * @return Long with flags.
     */
    private long flags(long pageAddr) {
        assert supportFlags();

        return PageUtils.getLong(pageAddr, FLAGS_OFFSET);
    }

    /**
     * @param pageAddr Page address.
     * @param unwrappedPk unwrapped primary key of this tree flag.
     * @param inlineObjSupported inline POJO by created tree flag.
     */
    public void setFlags(long pageAddr, boolean unwrappedPk, boolean inlineObjSupported) {
        assert supportFlags();

        long flags = unwrappedPk ? FLAG_UNWRAPPED_PK : 0;
        flags |= inlineObjSupported ? FLAG_INLINE_OBJECT_SUPPORTED : 0;

        PageUtils.putLong(pageAddr, FLAGS_OFFSET, flags);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("BPlusMeta [\n\tlevelsCnt=").a(getLevelsCount(addr))
            .a(",\n\trootLvl=").a(getRootLevel(addr))
            .a(",\n\tinlineSize=").a(getInlineSize(addr))
            .a("\n]");
            //TODO print firstPageIds by level
    }

    /**
     * @param pageAddr Page address.
     * @param inlineObjSupported Supports inline object flag.
     * @param unwrappedPk Unwrap PK flag.
     * @param pageSize Page size.
     */
    public static void upgradePageVersion(long pageAddr, boolean inlineObjSupported, boolean unwrappedPk, int pageSize) {
        BPlusMetaIO ioPrev = VERSIONS.forPage(pageAddr);

        long[] lvls = new long[ioPrev.getLevelsCount(pageAddr)];

        for (int i = 0; i < lvls.length; ++i)
            lvls[i] = ioPrev.getFirstPageId(pageAddr, i);

        int inlineSize = ioPrev.getInlineSize(pageAddr);

        BPlusMetaIO ioNew = VERSIONS.latest();

        setVersion(pageAddr, VERSIONS.latest().getVersion());

        ioNew.setLevelsCount(pageAddr, lvls.length, pageSize);

        for (int i = 0; i < lvls.length; ++i)
            ioNew.setFirstPageId(pageAddr, i, lvls[i]);

        ioNew.setInlineSize(pageAddr, inlineSize);
        ioNew.setCreatedVersion(pageAddr, IgniteVersionUtils.VER);
        ioNew.setFlags(pageAddr, unwrappedPk, inlineObjSupported);
    }
}
