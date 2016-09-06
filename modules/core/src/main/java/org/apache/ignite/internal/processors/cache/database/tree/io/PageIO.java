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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.database.MetadataStorage;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeInnerIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;

/**
 * Base format for all the page types.
 *
 * Checklist for page IO implementations and usage (The Rules):
 *
 * 1. IO should not have any `public static` methods.
 *    We have versioned IOs and any static method will mean that it have to always work in backward
 *    compatible way between all the IO versions. The base class {@link PageIO} has
 *    static methods (like {@code {@link #getPageId(ByteBuffer)}}) intentionally:
 *    this base format can not be changed between versions.
 *
 * 2. IO must correctly override {@link #initNewPage(ByteBuffer, long)} method and call super.
 *    We have logic that relies on this behavior.
 *
 * 3. Page IO type ID constant must be declared in this class to have a list of all the
 *    existing IO types in a single place.
 *
 * 4. IO must be added to {@link #getBPlusIO(int, int)} or to {@link #getPageIO(int, int)}.
 *
 * 5. Always keep in mind that IOs are versioned and their format can change from version
 *    to version. In this respect it is a good practice to avoid exposing details
 *    of IO internal format on it's API. The API should be minimalistic and abstract, so that
 *    internal format in future IO version can be completely changed without any changes
 *    to the API of this page IO.
 *
 * 6. Page IO API should not have any version dependent semantics and should not change API
 *    semantics in newer versions.
 *
 * 7. It is almost always preferable to read or write (especially write) page contents using
 *    static methods on {@link PageHandler}. To just initialize new page use
 *    {@link PageHandler#writePage(long, Page, PageHandler, PageIO, IgniteWriteAheadLogManager, Object, int)}
 *    method with needed IO instance and {@link PageHandler#NOOP} handler.
 */
public abstract class PageIO {
    /** */
    private static IOVersions<? extends BPlusInnerIO<?>> h2InnerIOs;

    /** */
    private static IOVersions<? extends BPlusLeafIO<?>> h2LeafIOs;

    /** */
    private static final int TYPE_OFF = 0;

    /** */
    private static final int VER_OFF = TYPE_OFF + 2;

    /** */
    private static final int CRC_OFF = VER_OFF + 2;

    /** */
    private static final int PAGE_ID_OFF = CRC_OFF + 4;

    /** */
    private static final int RESERVED_1_OFF = PAGE_ID_OFF + 8;

    /** */
    private static final int RESERVED_2_OFF = RESERVED_1_OFF + 8;

    /** */
    private static final int RESERVED_3_OFF = RESERVED_2_OFF + 8;

    /** */
    public static final int COMMON_HEADER_END = RESERVED_3_OFF + 8; // 40=type(2)+ver(2)+crc(4)+pageId(8)+reserved(3*8)

    /* All the page types. */

    /** */
    public static final short T_DATA = 1;

    /** */
    public static final short T_BPLUS_META = 2;

    /** */
    public static final short T_H2_REF_LEAF = 3;

    /** */
    public static final short T_H2_REF_INNER = 4;

    /** */
    public static final short T_DATA_REF_INNER = 5;

    /** */
    public static final short T_DATA_REF_LEAF = 6;

    /** */
    public static final short T_FREE_LEAF = 7;

    /** */
    public static final short T_FREE_INNER = 8;

    /** */
    public static final short T_REUSE_LEAF = 9;

    /** */
    public static final short T_REUSE_INNER = 10;

    /** */
    public static final short T_METASTORE_INNER = 11;

    /** */
    public static final short T_METASTORE_LEAF = 12;

    /** */
    public static final short T_PENDING_REF_INNER = 13;

    /** */
    public static final short T_PENDING_REF_LEAF = 14;

    /** */
    public static final short T_META = 15;

    /** */
    private final int ver;

    /** */
    private final int type;

    /**
     * @param type Page type.
     * @param ver Page format version.
     */
    protected PageIO(int type, int ver) {
        assert ver > 0 && ver < 65535: ver;
        assert type > 0 && type < 65535: type;

        this.type = type;
        this.ver = ver;
    }

    /**
     * @return Page type.
     */
    public static int getType(ByteBuffer buf) {
        return buf.getShort(TYPE_OFF) & 0xFFFF;
    }

    /**
     * @param buf Buffer.
     * @param type Type.
     */
    public static void setType(ByteBuffer buf, int type) {
        buf.putShort(TYPE_OFF, (short)type);

        assert getType(buf) == type;
    }

    /**
     * @param buf Buffer.
     * @return Version.
     */
    public static int getVersion(ByteBuffer buf) {
        return buf.getShort(VER_OFF) & 0xFFFF;
    }

    /**
     * @param buf Buffer.
     * @param ver Version.
     */
    public static void setVersion(ByteBuffer buf, int ver) {
        buf.putShort(VER_OFF, (short)ver);

        assert getVersion(buf) == ver;
    }

    /**
     * @param buf Buffer.
     * @return Page ID.
     */
    public static long getPageId(ByteBuffer buf) {
        return buf.getLong(PAGE_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param pageId Page ID.
     */
    public static void setPageId(ByteBuffer buf, long pageId) {
        buf.putLong(PAGE_ID_OFF, pageId);

        assert getPageId(buf) == pageId;
    }

    /**
     * @param buf Buffer.
     * @return Checksum.
     */
    public static int getCrc(ByteBuffer buf) {
        return buf.getInt(CRC_OFF);
    }

    /**
     * @param buf Buffer.
     * @param crc Checksum.
     */
    public static void setCrc(ByteBuffer buf, int crc) {
        buf.putInt(CRC_OFF, crc);
    }

    /**
     * Registers this B+Tree IO versions.
     *
     * @param innerIOs Inner IO versions.
     * @param leafIOs Leaf IO versions.
     */
    public static void registerH2(
        IOVersions<? extends BPlusInnerIO<?>> innerIOs,
        IOVersions<? extends BPlusLeafIO<?>> leafIOs
    ) {
        h2InnerIOs = innerIOs;
        h2LeafIOs = leafIOs;
    }

    /**
     * @return Type.
     */
    public final int getType() {
        return type;
    }

    /**
     * @return Version.
     */
    public final int getVersion() {
        return ver;
    }

    /**
     * @param buf Buffer.
     * @param pageId Page ID.
     */
    public void initNewPage(ByteBuffer buf, long pageId) {
        setType(buf, getType());
        setVersion(buf, getVersion());
        setPageId(buf, pageId);
        setCrc(buf, 0);

        buf.putLong(RESERVED_1_OFF, 0L);
        buf.putLong(RESERVED_2_OFF, 0L);
        buf.putLong(RESERVED_3_OFF, 0L);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + "[ver=" + getVersion() + "]";
    }

    /**
     * @param type IO Type.
     * @param ver IO Version.
     * @return Page IO.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public static <Q extends PageIO> Q getPageIO(int type, int ver) throws IgniteCheckedException {
        switch (type) {
            case T_DATA:
                return (Q)DataPageIO.VERSIONS.forVersion(ver);

            case T_BPLUS_META:
                return (Q)BPlusMetaIO.VERSIONS.forVersion(ver);

            case T_META:
                throw new IgniteCheckedException("Root meta page should be always accessed with a fixed version.");

            default:
                return (Q)getBPlusIO(type, ver);
        }
    }

    /**
     * @param buf Buffer.
     * @return IO for either inner or leaf B+Tree page.
     * @throws IgniteCheckedException If failed.
     */
    public static <Q extends BPlusIO<?>> Q getBPlusIO(ByteBuffer buf) throws IgniteCheckedException {
        int type = getType(buf);
        int ver = getVersion(buf);

        return getBPlusIO(type, ver);
    }

    /**
     * @param type IO Type.
     * @param ver IO Version.
     * @return IO for either inner or leaf B+Tree page.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public static <Q extends BPlusIO<?>> Q getBPlusIO(int type, int ver) throws IgniteCheckedException {
        switch (type) {
            case T_H2_REF_INNER:
                if (h2InnerIOs == null)
                    break;

                return (Q)h2InnerIOs.forVersion(ver);

            case T_H2_REF_LEAF:
                if (h2LeafIOs == null)
                    break;

                return (Q)h2LeafIOs.forVersion(ver);

            case T_DATA_REF_INNER:
                return (Q)IgniteCacheOffheapManagerImpl.DataInnerIO.VERSIONS.forVersion(ver);

            case T_DATA_REF_LEAF:
                return (Q)IgniteCacheOffheapManagerImpl.DataLeafIO.VERSIONS.forVersion(ver);

            case T_METASTORE_INNER:
                return (Q)MetadataStorage.MetaStoreInnerIO.VERSIONS.forVersion(ver);

            case T_METASTORE_LEAF:
                return (Q)MetadataStorage.MetaStoreLeafIO.VERSIONS.forVersion(ver);

            case T_FREE_INNER:
                return (Q)FreeInnerIO.VERSIONS.forVersion(ver);

            case T_FREE_LEAF:
                return (Q)FreeLeafIO.VERSIONS.forVersion(ver);

            case T_REUSE_INNER:
                return (Q)ReuseInnerIO.VERSIONS.forVersion(ver);

            case T_REUSE_LEAF:
                return (Q)ReuseLeafIO.VERSIONS.forVersion(ver);
        }

        throw new IgniteCheckedException("Unknown page IO type: " + type);
    }
}
