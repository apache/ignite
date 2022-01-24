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

package org.apache.ignite.internal.pagememory.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Base format for all page types.
 *
 * <p>Checklist for {@code PageIo} implementations and usage (The Rules):
 *
 * <ol>
 *     <li>
 *         IO should not have any {@code public static} methods.
 *
 *         IO implementations are versioned and static methods make it difficult to implement correctly in a backward-compatible way.
 *         The base {@link PageIo} class has some static methods (like {@link #getPageId(long)}) intentionally:
 *         this base format can not be changed between versions;
 *     </li>
 *     <li>
 *         IO must correctly override {@link #initNewPage(long, long, int)} method and call the super method;
 *     </li>
 *     <li>
 *         Always keep in mind that IOs are versioned and their format can change from version to version. In this respect it is a good
 *         practice to avoid exposing details of the internal format through the API. The API should be minimalistic and abstract,
 *         so the internal format in future IO versions can be changed without any changes to the API of this page IO;
 *     </li>
 *     <li>
 *         Page IO API should not have any version dependent semantics and should not change API semantics in newer versions;
 *     </li>
 *     <li>
 *         It is almost always preferable to read or write (especially write) page contents using static methods declared in
 *         {@link PageHandler}. To initialize a new page use {@link PageHandler#initPage} method with a corresponding IO instance.
 *     </li>
 * </ol>
 */
public abstract class PageIo {
    /**
     * Maximum value for page type.
     */
    public static final int MAX_IO_TYPE = 65535 - 1;

    /**
     * Offset for "short" page type.
     */
    public static final int TYPE_OFF = 0;

    /**
     * Offset for "short" page version.
     */
    public static final int VER_OFF = TYPE_OFF + Short.BYTES;

    /**
     * Offset for "int" CRC.
     */
    public static final int CRC_OFF = VER_OFF + Short.BYTES;

    /**
     * Offset for "long" page ID.
     */
    public static final int PAGE_ID_OFF = CRC_OFF + Integer.BYTES;

    /**
     * Offset for "byte" rotated ID.
     */
    public static final int ROTATED_ID_PART_OFF = PAGE_ID_OFF + Long.BYTES;

    /**
     * Offset for "byte" compression type.
     */
    private static final int COMPRESSION_TYPE_OFF = ROTATED_ID_PART_OFF + Byte.BYTES;

    /**
     * Offset for "short" compressed size.
     */
    private static final int COMPRESSED_SIZE_OFF = COMPRESSION_TYPE_OFF + Byte.BYTES;

    /**
     * Offset for "short" compacted size.
     */
    private static final int COMPACTED_SIZE_OFF = COMPRESSED_SIZE_OFF + Short.BYTES;

    /**
     * Offset for reserved "short" value.
     */
    private static final int RESERVED_SHORT_OFF = COMPACTED_SIZE_OFF + Short.BYTES;

    /**
     * Offset for reserved "long" value.
     */
    private static final int RESERVED_2_OFF = RESERVED_SHORT_OFF + Short.BYTES;

    /**
     * Offset for reserved "long" value.
     */
    private static final int RESERVED_3_OFF = RESERVED_2_OFF + Long.BYTES;

    /**
     * Total size of common header, including reserved bytes.
     */
    public static final int COMMON_HEADER_END = RESERVED_3_OFF + Long.BYTES;

    /**
     * IO version.
     */
    private final int ver;

    /**
     * IO type.
     */
    private final int type;

    /**
     * Constructor.
     *
     * @param type Page type.
     * @param ver  Page format version.
     */
    protected PageIo(int type, int ver) {
        assert ver > 0 && ver <= MAX_IO_TYPE : ver;
        assert type > 0 && type <= MAX_IO_TYPE : type;

        this.type = type;
        this.ver = ver;
    }

    /**
     * Returns a type.
     */
    public final int getType() {
        return type;
    }

    /**
     * Returns a page type.
     *
     * @param buf Buffer.
     * @return Page type.
     */
    public static int getType(ByteBuffer buf) {
        return buf.getShort(TYPE_OFF) & 0xFFFF;
    }

    /**
     * Returns a page type.
     *
     * @param pageAddr Page address.
     * @return Page type.
     */
    public static int getType(long pageAddr) {
        return PageUtils.getShort(pageAddr, TYPE_OFF) & 0xFFFF;
    }

    /**
     * Sets the type to the page.
     *
     * @param pageAddr Page address.
     * @param type     Type.
     */
    public static void setType(long pageAddr, int type) {
        PageUtils.putShort(pageAddr, TYPE_OFF, (short) type);

        assert getType(pageAddr) == type : getType(pageAddr);
    }

    /**
     * Returns a version.
     */
    public final int getVersion() {
        return ver;
    }

    /**
     * Returns a page version.
     *
     * @param buf Buffer.
     * @return Version.
     */
    public static int getVersion(ByteBuffer buf) {
        return buf.getShort(VER_OFF) & 0xFFFF;
    }

    /**
     * Returns a page version.
     *
     * @param pageAddr Page address.
     * @return Version.
     */
    public static int getVersion(long pageAddr) {
        return PageUtils.getShort(pageAddr, VER_OFF) & 0xFFFF;
    }

    /**
     * Sets the version to the page.
     *
     * @param pageAddr Page address.
     * @param ver      Version.
     */
    protected static void setVersion(long pageAddr, int ver) {
        PageUtils.putShort(pageAddr, VER_OFF, (short) ver);

        assert getVersion(pageAddr) == ver;
    }

    /**
     * Returns a page ID.
     *
     * @param buf Buffer.
     * @return Page ID.
     */
    public static long getPageId(ByteBuffer buf) {
        return buf.getLong(PAGE_ID_OFF);
    }

    /**
     * Returns a page ID.
     *
     * @param pageAddr Page address.
     * @return Page ID.
     */
    public static long getPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, PAGE_ID_OFF);
    }

    /**
     * Sets the page ID to the page.
     *
     * @param pageAddr Page address.
     * @param pageId   Page ID.
     */
    public static void setPageId(long pageAddr, long pageId) {
        PageUtils.putLong(pageAddr, PAGE_ID_OFF, pageId);

        assert getPageId(pageAddr) == pageId;
    }

    /**
     * Returns a rotated ID.
     *
     * @param pageAddr Page address.
     * @return Rotated page ID part.
     */
    public static int getRotatedIdPart(long pageAddr) {
        return PageUtils.getUnsignedByte(pageAddr, ROTATED_ID_PART_OFF);
    }

    /**
     * Sets the rotated ID to the page.
     *
     * @param pageAddr      Page address.
     * @param rotatedIdPart Rotated page ID part.
     */
    public static void setRotatedIdPart(long pageAddr, int rotatedIdPart) {
        PageUtils.putUnsignedByte(pageAddr, ROTATED_ID_PART_OFF, rotatedIdPart);

        assert getRotatedIdPart(pageAddr) == rotatedIdPart;
    }

    /**
     * Sets the compression type to the page.
     *
     * @param page         Page buffer.
     * @param compressType Compression type.
     */
    public static void setCompressionType(ByteBuffer page, byte compressType) {
        page.put(COMPRESSION_TYPE_OFF, compressType);
    }

    /**
     * Returns a compression type.
     *
     * @param page Page buffer.
     * @return Compression type.
     */
    public static byte getCompressionType(ByteBuffer page) {
        return page.get(COMPRESSION_TYPE_OFF);
    }

    /**
     * Returns a compression type.
     *
     * @param pageAddr Page address.
     * @return Compression type.
     */
    public static byte getCompressionType(long pageAddr) {
        return PageUtils.getByte(pageAddr, COMPRESSION_TYPE_OFF);
    }

    /**
     * Sets the compressed size to the page.
     *
     * @param page           Page buffer.
     * @param compressedSize Compressed size.
     */
    public static void setCompressedSize(ByteBuffer page, short compressedSize) {
        page.putShort(COMPRESSED_SIZE_OFF, compressedSize);
    }

    /**
     * Returns a compressed size.
     *
     * @param page Page buffer.
     * @return Compressed size.
     */
    public static short getCompressedSize(ByteBuffer page) {
        return page.getShort(COMPRESSED_SIZE_OFF);
    }

    /**
     * Returns a compressed size.
     *
     * @param pageAddr Page address.
     * @return Compressed size.
     */
    public static short getCompressedSize(long pageAddr) {
        return PageUtils.getShort(pageAddr, COMPRESSED_SIZE_OFF);
    }

    /**
     * Sets the compacted size to the page.
     *
     * @param page          Page buffer.
     * @param compactedSize Compacted size.
     */
    public static void setCompactedSize(ByteBuffer page, short compactedSize) {
        page.putShort(COMPACTED_SIZE_OFF, compactedSize);
    }

    /**
     * Returns a compacted size.
     *
     * @param page Page buffer.
     * @return Compacted size.
     */
    public static short getCompactedSize(ByteBuffer page) {
        return page.getShort(COMPACTED_SIZE_OFF);
    }

    /**
     * Returns a compacted size.
     *
     * @param pageAddr Page address.
     * @return Compacted size.
     */
    public static short getCompactedSize(long pageAddr) {
        return PageUtils.getShort(pageAddr, COMPACTED_SIZE_OFF);
    }

    /**
     * Sets the CRC value to the page.
     *
     * @param buf Buffer.
     * @param crc Checksum.
     */
    public static void setCrc(ByteBuffer buf, int crc) {
        buf.putInt(CRC_OFF, crc);
    }

    /**
     * Sets the CRC value to the page.
     *
     * @param pageAddr Page address.
     * @param crc      Checksum.
     */
    public static void setCrc(long pageAddr, int crc) {
        PageUtils.putInt(pageAddr, CRC_OFF, crc);
    }

    /**
     * Returns a CRC value.
     *
     * @param buf Buffer.
     * @return Checksum.
     */
    public static int getCrc(ByteBuffer buf) {
        return buf.getInt(CRC_OFF);
    }

    /**
     * Returns a CRC value.
     *
     * @param pageAddr Page address.
     * @return Checksum.
     */
    public static int getCrc(long pageAddr) {
        return PageUtils.getInt(pageAddr, CRC_OFF);
    }

    /**
     * Initializes a new page.
     *
     * @param pageAddr Page address.
     * @param pageId   Page ID.
     * @param pageSize Page size.
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
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[ver=" + getVersion() + "]";
    }

    /**
     * Copies a page into the output {@link ByteBuffer}.
     *
     * @param page     Page.
     * @param out      Output buffer.
     * @param pageSize Page size.
     */
    protected final void copyPage(ByteBuffer page, ByteBuffer out, int pageSize) {
        assert out.position() == 0;
        assert pageSize <= out.remaining();
        assert pageSize == page.remaining();

        PageUtils.copyMemory(page, 0, out, 0, pageSize);
        out.limit(pageSize);
    }

    /**
     * Prints a page into the output {@link StringBuilder}.
     *
     * @param addr     Address.
     * @param pageSize Page size.
     * @param sb       Sb.
     */
    protected abstract void printPage(long addr, int pageSize, StringBuilder sb) throws IgniteInternalCheckedException;

    /**
     * Returns a String representation of pages content.
     *
     * @param pageAddr Address.
     */
    public static String printPage(PageIoRegistry pageIoRegistry, long pageAddr, int pageSize) {
        StringBuilder sb = new StringBuilder("Header [\n\ttype=");

        try {
            PageIo io = pageIoRegistry.resolve(pageAddr);

            sb.append(getType(pageAddr))
                    .append(" (").append(io.getClass().getSimpleName())
                    .append("),\n\tver=").append(getVersion(pageAddr)).append(",\n\tcrc=").append(getCrc(pageAddr))
                    .append(",\n\t").append(PageIdUtils.toDetailString(getPageId(pageAddr)))
                    .append("\n],\n");

            if (getCompressionType(pageAddr) != 0) {
                sb.append("CompressedPage[\n\tcompressionType=").append(getCompressionType(pageAddr))
                        .append(",\n\tcompressedSize=").append(getCompressedSize(pageAddr))
                        .append(",\n\tcompactedSize=").append(getCompactedSize(pageAddr))
                        .append("\n]");
            } else {
                io.printPage(pageAddr, pageSize, sb);
            }
        } catch (IgniteInternalCheckedException e) {
            sb.append("Failed to print page: ").append(e.getMessage());
        }

        return sb.toString();
    }

    /**
     * Asserts that page type of the page stored at pageAddr matches page type of this PageIO.
     *
     * @param pageAddr address of a page to use for assertion
     */
    protected final void assertPageType(long pageAddr) {
        assert getType(pageAddr) == getType() : "Expected type " + getType() + ", but got " + getType(pageAddr);
    }

    /**
     * Asserts that page type of the page stored in the given buffer matches page type of this PageIO.
     *
     * @param buf buffer where the page for assertion is stored
     */
    protected final void assertPageType(ByteBuffer buf) {
        assert getType(buf) == getType() : "Expected type " + getType() + ", but got " + getType(buf);
    }
}
