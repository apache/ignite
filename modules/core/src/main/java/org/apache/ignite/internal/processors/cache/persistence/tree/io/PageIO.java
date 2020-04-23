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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.metric.IndexPageType;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLogInnerIO;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLogLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageBPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastoreDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.tree.CacheIdAwareDataInnerIO;
import org.apache.ignite.internal.processors.cache.tree.CacheIdAwareDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.CacheIdAwarePendingEntryInnerIO;
import org.apache.ignite.internal.processors.cache.tree.CacheIdAwarePendingEntryLeafIO;
import org.apache.ignite.internal.processors.cache.tree.DataInnerIO;
import org.apache.ignite.internal.processors.cache.tree.DataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.PendingEntryInnerIO;
import org.apache.ignite.internal.processors.cache.tree.PendingEntryLeafIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccCacheIdAwareDataInnerIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccCacheIdAwareDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataInnerIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataLeafIO;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.spi.encryption.EncryptionSpi;

/**
 * Base format for all the page types.
 *
 * Checklist for page IO implementations and usage (The Rules):
 *
 * 1. IO should not have any `public static` methods.
 *    We have versioned IOs and any static method will mean that it have to always work in backward
 *    compatible way between all the IO versions. The base class {@link PageIO} has
 *    static methods (like {@code {@link #getPageId(long)}}) intentionally:
 *    this base format can not be changed between versions.
 *
 * 2. IO must correctly override {@link #initNewPage(long, long, int)} method and call super.
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
 *    {@link PageHandler#initPage(PageMemory, int, long, PageIO, IgniteWriteAheadLogManager, PageLockListener, IoStatisticsHolder)}
 *    method with needed IO instance.
 */
public abstract class PageIO {
    /** */
    private static PageIO testIO;

    /** */
    private static BPlusInnerIO<?> innerTestIO;

    /** */
    private static BPlusLeafIO<?> leafTestIO;

    /** */
    private static IOVersions<? extends BPlusInnerIO<?>> h2InnerIOs;

    /** */
    private static IOVersions<? extends BPlusLeafIO<?>> h2LeafIOs;

    /** */
    private static IOVersions<? extends BPlusInnerIO<?>> h2MvccInnerIOs;

    /** */
    private static IOVersions<? extends BPlusLeafIO<?>> h2MvccLeafIOs;

    /** Maximum payload size. */
    public static final short MAX_PAYLOAD_SIZE = 2048;

    /** */
    private static List<IOVersions<? extends BPlusInnerIO<?>>> h2ExtraInnerIOs = new ArrayList<>(MAX_PAYLOAD_SIZE);

    /** */
    private static List<IOVersions<? extends BPlusLeafIO<?>>> h2ExtraLeafIOs = new ArrayList<>(MAX_PAYLOAD_SIZE);

    /** */
    private static List<IOVersions<? extends BPlusInnerIO<?>>> h2ExtraMvccInnerIOs = new ArrayList<>(MAX_PAYLOAD_SIZE);

    /** */
    private static List<IOVersions<? extends BPlusLeafIO<?>>> h2ExtraMvccLeafIOs = new ArrayList<>(MAX_PAYLOAD_SIZE);

    /** */
    public static final int TYPE_OFF = 0;

    /** */
    public static final int VER_OFF = TYPE_OFF + 2;

    /** */
    public static final int CRC_OFF = VER_OFF + 2;

    /** */
    public static final int PAGE_ID_OFF = CRC_OFF + 4;

    /** */
    public static final int ROTATED_ID_PART_OFF = PAGE_ID_OFF + 8;

    /** */
    private static final int COMPRESSION_TYPE_OFF = ROTATED_ID_PART_OFF + 1;

    /** */
    private static final int COMPRESSED_SIZE_OFF = COMPRESSION_TYPE_OFF + 1;

    /** */
    private static final int COMPACTED_SIZE_OFF = COMPRESSED_SIZE_OFF + 2;

    /** */
    private static final int RESERVED_SHORT_OFF = COMPACTED_SIZE_OFF + 2;

    /** */
    private static final int RESERVED_2_OFF = RESERVED_SHORT_OFF + 2;

    /** */
    private static final int RESERVED_3_OFF = RESERVED_2_OFF + 8;

    /** */
    public static final int COMMON_HEADER_END = RESERVED_3_OFF + 8; // 40=type(2)+ver(2)+crc(4)+pageId(8)+rotatedIdPart(1)+reserved(1+2+4+2*8)

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
    public static final short T_METASTORE_INNER = 7;

    /** */
    public static final short T_METASTORE_LEAF = 8;

    /** */
    public static final short T_PENDING_REF_INNER = 9;

    /** */
    public static final short T_PENDING_REF_LEAF = 10;

    /** */
    public static final short T_META = 11;

    /** */
    public static final short T_PAGE_LIST_META = 12;

    /** */
    public static final short T_PAGE_LIST_NODE = 13;

    /** */
    public static final short T_PART_META = 14;

    /** */
    public static final short T_PAGE_UPDATE_TRACKING = 15;

    /** */
    public static final short T_CACHE_ID_AWARE_DATA_REF_INNER = 16;

    /** */
    public static final short T_CACHE_ID_AWARE_DATA_REF_LEAF = 17;

    /** */
    public static final short T_CACHE_ID_AWARE_PENDING_REF_INNER = 18;

    /** */
    public static final short T_CACHE_ID_AWARE_PENDING_REF_LEAF = 19;

    /** */
    public static final short T_PART_CNTRS = 20;

    /** */
    public static final short T_DATA_METASTORAGE = 21;

    /** */
    public static final short T_DATA_REF_METASTORAGE_INNER = 22;

    /** */
    public static final short T_DATA_REF_METASTORAGE_LEAF = 23;

    /** */
    public static final short T_DATA_REF_MVCC_INNER = 24;

    /** */
    public static final short T_DATA_REF_MVCC_LEAF = 25;

    /** */
    public static final short T_CACHE_ID_DATA_REF_MVCC_INNER = 26;

    /** */
    public static final short T_CACHE_ID_DATA_REF_MVCC_LEAF = 27;

    /** */
    public static final short T_H2_MVCC_REF_LEAF = 28;

    /** */
    public static final short T_H2_MVCC_REF_INNER = 29;

    /** */
    public static final short T_TX_LOG_LEAF = 30;

    /** */
    public static final short T_TX_LOG_INNER = 31;

    /** */
    public static final short T_DATA_PART = 32;

    /** Index for payload == 1. */
    public static final short T_H2_EX_REF_LEAF_START = 10_000;

    /** */
    public static final short T_H2_EX_REF_LEAF_END = T_H2_EX_REF_LEAF_START + MAX_PAYLOAD_SIZE - 1;

    /** */
    public static final short T_H2_EX_REF_INNER_START = 20_000;

    /** */
    public static final short T_H2_EX_REF_INNER_END = T_H2_EX_REF_INNER_START + MAX_PAYLOAD_SIZE - 1;

    /** */
    public static final short T_H2_EX_REF_MVCC_LEAF_START = 23_000;

    /** */
    public static final short T_H2_EX_REF_MVCC_LEAF_END = T_H2_EX_REF_MVCC_LEAF_START + MAX_PAYLOAD_SIZE - 1;

    /** */
    public static final short T_H2_EX_REF_MVCC_INNER_START = 26_000;

    /** */
    public static final short T_H2_EX_REF_MVCC_INNER_END = T_H2_EX_REF_MVCC_INNER_START + MAX_PAYLOAD_SIZE - 1;

    /** */
    private final int ver;

    /** */
    private final int type;

    /**
     * @param type Page type.
     * @param ver Page format version.
     */
    protected PageIO(int type, int ver) {
        assert ver > 0 && ver < 65535 : ver;
        assert type > 0 && type < 65535 : type;

        this.type = type;
        this.ver = ver;
    }

    /**
     * @param buf Buffer.
     * @return Page type.
     */
    public static int getType(ByteBuffer buf) {
        return buf.getShort(TYPE_OFF) & 0xFFFF;
    }

    /**
     * @param pageAddr Page address.
     * @return Page type.
     */
    public static int getType(long pageAddr) {
        return PageUtils.getShort(pageAddr, TYPE_OFF) & 0xFFFF;
    }

    /**
     * @param pageAddr Page address.
     * @param type Type.
     */
    public static void setType(long pageAddr, int type) {
        PageUtils.putShort(pageAddr, TYPE_OFF, (short)type);

        assert getType(pageAddr) == type : getType(pageAddr);
    }

    /**
     * @param buf Buffer.
     * @return Version.
     */
    public static int getVersion(ByteBuffer buf) {
        return buf.getShort(VER_OFF) & 0xFFFF;
    }

    /**
     * @param pageAddr Page address.
     * @return Version.
     */
    public static int getVersion(long pageAddr) {
        return PageUtils.getShort(pageAddr, VER_OFF) & 0xFFFF;
    }

    /**
     * @param pageAddr Page address.
     * @param ver Version.
     */
    protected static void setVersion(long pageAddr, int ver) {
        PageUtils.putShort(pageAddr, VER_OFF, (short)ver);

        assert getVersion(pageAddr) == ver;
    }

    /**
     * @param buf Buffer.
     * @return Page ID.
     */
    public static long getPageId(ByteBuffer buf) {
        return buf.getLong(PAGE_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @return Page ID.
     */
    public static long getPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, PAGE_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageId Page ID.
     */
    public static void setPageId(long pageAddr, long pageId) {
        PageUtils.putLong(pageAddr, PAGE_ID_OFF, pageId);

        assert getPageId(pageAddr) == pageId;
    }

    /**
     * @param pageAddr Page address.
     * @return Rotated page ID part.
     */
    public static int getRotatedIdPart(long pageAddr) {
        return PageUtils.getUnsignedByte(pageAddr, ROTATED_ID_PART_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param rotatedIdPart Rotated page ID part.
     */
    public static void setRotatedIdPart(long pageAddr, int rotatedIdPart) {
        PageUtils.putUnsignedByte(pageAddr, ROTATED_ID_PART_OFF, rotatedIdPart);

        assert getRotatedIdPart(pageAddr) == rotatedIdPart;
    }

    /**
     * @param page Page buffer.
     * @param compressType Compression type.
     */
    public static void setCompressionType(ByteBuffer page, byte compressType) {
        page.put(COMPRESSION_TYPE_OFF, compressType);
    }

    /**
     * @param page Page buffer.
     * @return Compression type.
     */
    public static byte getCompressionType(ByteBuffer page) {
        return page.get(COMPRESSION_TYPE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @return Compression type.
     */
    public static byte getCompressionType(long pageAddr) {
        return PageUtils.getByte(pageAddr, COMPRESSION_TYPE_OFF);
    }

    /**
     * @param page Page buffer.
     * @param compressedSize Compressed size.
     */
    public static void setCompressedSize(ByteBuffer page, short compressedSize) {
        page.putShort(COMPRESSED_SIZE_OFF, compressedSize);
    }

    /**
     * @param page Page buffer.
     * @return Compressed size.
     */
    public static short getCompressedSize(ByteBuffer page) {
        return page.getShort(COMPRESSED_SIZE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @return Compressed size.
     */
    public static short getCompressedSize(long pageAddr) {
        return PageUtils.getShort(pageAddr, COMPRESSED_SIZE_OFF);
    }

    /**
     * @param page Page buffer.
     * @param compactedSize Compacted size.
     */
    public static void setCompactedSize(ByteBuffer page, short compactedSize) {
        page.putShort(COMPACTED_SIZE_OFF, compactedSize);
    }

    /**
     * @param page Page buffer.
     * @return Compacted size.
     */
    public static short getCompactedSize(ByteBuffer page) {
        return page.getShort(COMPACTED_SIZE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @return Compacted size.
     */
    public static short getCompactedSize(long pageAddr) {
        return PageUtils.getShort(pageAddr, COMPACTED_SIZE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @return Checksum.
     */
    public static int getCrc(long pageAddr) {
        return PageUtils.getInt(pageAddr, CRC_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param crc Checksum.
     */
    public static void setCrc(long pageAddr, int crc) {
        PageUtils.putInt(pageAddr, CRC_OFF, crc);
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
     * @param mvccInnerIOs Inner IO versions with mvcc enabled.
     * @param mvccLeafIOs Leaf IO versions with mvcc enabled.
     */
    public static void registerH2(
        IOVersions<? extends BPlusInnerIO<?>> innerIOs,
        IOVersions<? extends BPlusLeafIO<?>> leafIOs,
        IOVersions<? extends BPlusInnerIO<?>> mvccInnerIOs,
        IOVersions<? extends BPlusLeafIO<?>> mvccLeafIOs
    ) {
        h2InnerIOs = innerIOs;
        h2LeafIOs = leafIOs;
        h2MvccInnerIOs = mvccInnerIOs;
        h2MvccLeafIOs = mvccLeafIOs;
    }

    /**
     * Registers extra inner IO versions.
     *
     * @param innerExtIOs Extra versions.
     */
    public static void registerH2ExtraInner(IOVersions<? extends BPlusInnerIO<?>> innerExtIOs, boolean mvcc) {
        List<IOVersions<? extends BPlusInnerIO<?>>> ios = mvcc ? h2ExtraMvccInnerIOs : h2ExtraInnerIOs;

        ios.add(innerExtIOs);
    }

    /**
     * Registers extra inner IO versions.
     *
     * @param leafExtIOs Extra versions.
     */
    public static void registerH2ExtraLeaf(IOVersions<? extends BPlusLeafIO<?>> leafExtIOs, boolean mvcc) {
        List<IOVersions<? extends BPlusLeafIO<?>>> ios = mvcc ? h2ExtraMvccLeafIOs : h2ExtraLeafIOs;

        ios.add(leafExtIOs);
    }

    /**
     * @param idx Index.
     * @return IOVersions for given idx.
     */
    public static IOVersions<? extends BPlusInnerIO<?>> getInnerVersions(int idx, boolean mvcc) {
        List<IOVersions<? extends BPlusInnerIO<?>>> ios = mvcc ? h2ExtraMvccInnerIOs : h2ExtraInnerIOs;

        return ios.get(idx);
    }

    /**
     * @param idx Index.
     * @return IOVersions for given idx.
     */
    public static IOVersions<? extends BPlusLeafIO<?>> getLeafVersions(int idx, boolean mvcc) {
        List<IOVersions<? extends BPlusLeafIO<?>>> ios = mvcc ? h2ExtraMvccLeafIOs : h2ExtraLeafIOs;

        return ios.get(idx);
    }

    /**
     * Registers IOs for testing.
     *
     * @param innerIO Inner IO.
     * @param leafIO Leaf IO.
     */
    public static void registerTest(BPlusInnerIO<?> innerIO, BPlusLeafIO<?> leafIO) {
        innerTestIO = innerIO;
        leafTestIO = leafIO;
    }

    /**
     * Registers IO for testing.
     *
     * @param io Page IO.
     */
    public static void registerTest(PageIO io) {
        testIO = io;
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
     * @param pageAddr Page address.
     * @param pageId Page ID.
     * @param pageSize Page size.
     *
     * @see EncryptionSpi#encryptedSize(int)
     */
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        setType(pageAddr, getType());
        setVersion(pageAddr, getVersion());
        setPageId(pageAddr, pageId);
        setCrc(pageAddr, 0);

        // rotated(1) + compress_type(1) + compressed_size(2) + compacted_size(2) + reserved(2)
        PageUtils.putLong(pageAddr, ROTATED_ID_PART_OFF, 0L);
        PageUtils.putLong(pageAddr, RESERVED_2_OFF, 0L);
        PageUtils.putLong(pageAddr, RESERVED_3_OFF, 0L);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + "[ver=" + getVersion() + "]";
    }

    /**
     * @param pageAddr Page address.
     * @return IO.
     * @throws IgniteCheckedException If failed.
     */
    public static <Q extends PageIO> Q getPageIO(long pageAddr) throws IgniteCheckedException {
        int type = getType(pageAddr);
        int ver = getVersion(pageAddr);

        return getPageIO(type, ver);
    }

    /**
     * @param page Page.
     * @return Page IO.
     * @throws IgniteCheckedException If failed.
     */
    public static <Q extends PageIO> Q getPageIO(ByteBuffer page) throws IgniteCheckedException {
        return getPageIO(getType(page), getVersion(page));
    }

    /**
     * @param type IO Type.
     * @param ver IO Version.
     * @return Page IO.
     * @throws IgniteCheckedException If failed.
     */
    public static <Q extends PageIO> Q getPageIO(int type, int ver) throws IgniteCheckedException {
        switch (type) {
            case T_DATA:
                return (Q)DataPageIO.VERSIONS.forVersion(ver);

            case T_BPLUS_META:
                return (Q)BPlusMetaIO.VERSIONS.forVersion(ver);

            case T_PAGE_LIST_NODE:
                return (Q)PagesListNodeIO.VERSIONS.forVersion(ver);

            case T_PAGE_LIST_META:
                return (Q)PagesListMetaIO.VERSIONS.forVersion(ver);

            case T_META:
                return (Q)PageMetaIO.VERSIONS.forVersion(ver);

            case T_PART_META:
                return (Q)PagePartitionMetaIO.VERSIONS.forVersion(ver);

            case T_PART_CNTRS:
                return (Q)PagePartitionCountersIO.VERSIONS.forVersion(ver);

            case T_PAGE_UPDATE_TRACKING:
                return (Q)TrackingPageIO.VERSIONS.forVersion(ver);

            case T_DATA_METASTORAGE:
                return (Q)MetastoreDataPageIO.VERSIONS.forVersion(ver);

            case T_DATA_PART:
                return (Q)SimpleDataPageIO.VERSIONS.forVersion(ver);

            default:
                if (testIO != null) {
                    if (testIO.type == type && testIO.ver == ver)
                        return (Q)testIO;
                }

                return (Q)getBPlusIO(type, ver);
        }
    }

    /**
     * @param pageAddr Page address.
     * @return IO for either inner or leaf B+Tree page.
     * @throws IgniteCheckedException If failed.
     */
    public static <Q extends BPlusIO<?>> Q getBPlusIO(long pageAddr) throws IgniteCheckedException {
        int type = getType(pageAddr);
        int ver = getVersion(pageAddr);

        return getBPlusIO(type, ver);
    }

    /**
     * @param type IO Type.
     * @param ver IO Version.
     * @return IO for either inner or leaf B+Tree page.
     * @throws IgniteCheckedException If failed.
     */
    public static <Q extends BPlusIO<?>> Q getBPlusIO(int type, int ver) throws IgniteCheckedException {
        if (type >= T_H2_EX_REF_LEAF_START && type <= T_H2_EX_REF_LEAF_END)
            return (Q)h2ExtraLeafIOs.get(type - T_H2_EX_REF_LEAF_START).forVersion(ver);

        if (type >= T_H2_EX_REF_INNER_START && type <= T_H2_EX_REF_INNER_END)
            return (Q)h2ExtraInnerIOs.get(type - T_H2_EX_REF_INNER_START).forVersion(ver);

        if (type >= T_H2_EX_REF_MVCC_LEAF_START && type <= T_H2_EX_REF_MVCC_LEAF_END)
            return (Q)h2ExtraMvccLeafIOs.get(type - T_H2_EX_REF_MVCC_LEAF_START).forVersion(ver);

        if (type >= T_H2_EX_REF_MVCC_INNER_START && type <= T_H2_EX_REF_MVCC_INNER_END)
            return (Q)h2ExtraMvccInnerIOs.get(type - T_H2_EX_REF_MVCC_INNER_START).forVersion(ver);

        switch (type) {
            case T_H2_REF_INNER:
                if (h2InnerIOs == null)
                    break;

                return (Q)h2InnerIOs.forVersion(ver);

            case T_H2_REF_LEAF:
                if (h2LeafIOs == null)
                    break;

                return (Q)h2LeafIOs.forVersion(ver);

            case T_H2_MVCC_REF_INNER:
                if (h2MvccInnerIOs == null)
                    break;

                return (Q)h2MvccInnerIOs.forVersion(ver);

            case T_H2_MVCC_REF_LEAF:
                if (h2MvccLeafIOs == null)
                    break;

                return (Q)h2MvccLeafIOs.forVersion(ver);

            case T_TX_LOG_INNER:
                return (Q)TxLogInnerIO.VERSIONS.forVersion(ver);

            case T_TX_LOG_LEAF:
                return (Q)TxLogLeafIO.VERSIONS.forVersion(ver);

            case T_DATA_REF_INNER:
                return (Q)DataInnerIO.VERSIONS.forVersion(ver);

            case T_DATA_REF_LEAF:
                return (Q)DataLeafIO.VERSIONS.forVersion(ver);

            case T_CACHE_ID_AWARE_DATA_REF_INNER:
                return (Q)CacheIdAwareDataInnerIO.VERSIONS.forVersion(ver);

            case T_CACHE_ID_AWARE_DATA_REF_LEAF:
                return (Q)CacheIdAwareDataLeafIO.VERSIONS.forVersion(ver);

            case T_CACHE_ID_DATA_REF_MVCC_INNER:
                return (Q) MvccCacheIdAwareDataInnerIO.VERSIONS.forVersion(ver);

            case T_CACHE_ID_DATA_REF_MVCC_LEAF:
                return (Q) MvccCacheIdAwareDataLeafIO.VERSIONS.forVersion(ver);

            case T_DATA_REF_MVCC_INNER:
                return (Q)MvccDataInnerIO.VERSIONS.forVersion(ver);

            case T_DATA_REF_MVCC_LEAF:
                return (Q)MvccDataLeafIO.VERSIONS.forVersion(ver);

            case T_METASTORE_INNER:
                return (Q)IndexStorageImpl.MetaStoreInnerIO.VERSIONS.forVersion(ver);

            case T_METASTORE_LEAF:
                return (Q)IndexStorageImpl.MetaStoreLeafIO.VERSIONS.forVersion(ver);

            case T_PENDING_REF_INNER:
                return (Q)PendingEntryInnerIO.VERSIONS.forVersion(ver);

            case T_PENDING_REF_LEAF:
                return (Q)PendingEntryLeafIO.VERSIONS.forVersion(ver);

            case T_CACHE_ID_AWARE_PENDING_REF_INNER:
                return (Q)CacheIdAwarePendingEntryInnerIO.VERSIONS.forVersion(ver);

            case T_CACHE_ID_AWARE_PENDING_REF_LEAF:
                return (Q)CacheIdAwarePendingEntryLeafIO.VERSIONS.forVersion(ver);

            case T_DATA_REF_METASTORAGE_INNER:
                return (Q)MetastorageBPlusIO.INNER_IO_VERSIONS.forVersion(ver);

            case T_DATA_REF_METASTORAGE_LEAF:
                return (Q)MetastorageBPlusIO.LEAF_IO_VERSIONS.forVersion(ver);

            default:
                // For tests.
                if (innerTestIO != null && innerTestIO.getType() == type && innerTestIO.getVersion() == ver)
                    return (Q)innerTestIO;

                if (leafTestIO != null && leafTestIO.getType() == type && leafTestIO.getVersion() == ver)
                    return (Q)leafTestIO;
        }

        throw new IgniteCheckedException("Unknown page IO type: " + type);
    }

    /**
     * @param pageAddr Address of page.
     * @return Index page type.
     */
    public static IndexPageType deriveIndexPageType(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);
        switch (pageIoType) {
            case PageIO.T_DATA_REF_INNER:
            case PageIO.T_DATA_REF_MVCC_INNER:
            case PageIO.T_H2_REF_INNER:
            case PageIO.T_H2_MVCC_REF_INNER:
            case PageIO.T_CACHE_ID_AWARE_DATA_REF_INNER:
            case PageIO.T_CACHE_ID_DATA_REF_MVCC_INNER:
                return IndexPageType.INNER;

            case PageIO.T_DATA_REF_LEAF:
            case PageIO.T_DATA_REF_MVCC_LEAF:
            case PageIO.T_H2_REF_LEAF:
            case PageIO.T_H2_MVCC_REF_LEAF:
            case PageIO.T_CACHE_ID_AWARE_DATA_REF_LEAF:
            case PageIO.T_CACHE_ID_DATA_REF_MVCC_LEAF:
                return IndexPageType.LEAF;

            default:
                if ((PageIO.T_H2_EX_REF_LEAF_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_LEAF_END) ||
                    (PageIO.T_H2_EX_REF_MVCC_LEAF_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_MVCC_LEAF_END)
                )
                    return IndexPageType.LEAF;

                if ((PageIO.T_H2_EX_REF_INNER_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_INNER_END) ||
                    (PageIO.T_H2_EX_REF_MVCC_INNER_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_MVCC_INNER_END)
                )
                    return IndexPageType.INNER;
        }

        return IndexPageType.NOT_INDEX;
    }

    /**
     * @param type Type to test.
     * @return {@code True} if data page.
     */
    public static boolean isDataPageType(int type) {
        return type == T_DATA;
    }

    /**
     * @param addr Address.
     * @param pageSize Page size.
     * @param sb Sb.
     */
    protected abstract void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException;

    /**
     * @param page Page.
     * @param out Output buffer.
     * @param pageSize Page size.
     */
    protected final void copyPage(ByteBuffer page, ByteBuffer out, int pageSize) {
        assert out.position() == 0;
        assert pageSize <= out.remaining();
        assert pageSize == page.remaining();

        PageHandler.copyMemory(page, 0, out, 0, pageSize);
        out.limit(pageSize);
    }

    /**
     * @param addr Address.
     */
    public static String printPage(long addr, int pageSize) {
        GridStringBuilder sb = new GridStringBuilder("Header [\n\ttype=");

        try {
            PageIO io = getPageIO(addr);

            sb.a(getType(addr)).a(" (").a(io.getClass().getSimpleName())
                .a("),\n\tver=").a(getVersion(addr)).a(",\n\tcrc=").a(getCrc(addr))
                .a(",\n\t").a(PageIdUtils.toDetailString(getPageId(addr)))
                .a("\n],\n");

            if (getCompressionType(addr) != 0) {
                sb.a("CompressedPage[\n\tcompressionType=").a(getCompressionType(addr))
                    .a(",\n\tcompressedSize=").a(getCompressedSize(addr))
                    .a(",\n\tcompactedSize=").a(getCompactedSize(addr))
                    .a("\n]");
            }
            else
                io.printPage(addr, pageSize, sb);
        }
        catch (IgniteCheckedException e) {
            sb.a("Failed to print page: ").a(e.getMessage());
        }

        return sb.toString();
    }
}
