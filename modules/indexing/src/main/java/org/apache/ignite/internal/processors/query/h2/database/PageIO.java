package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;

/**
 * Base format for all the page types.
 */
public abstract class PageIO {
    /** */
    private static final int TYPE_OFF = 0;

    /** */
    private static final int VER_OFF = TYPE_OFF + 2;

    /** */
    private static final int PAGE_ID_OFF = VER_OFF + 2;

    /** */
    private static final int CRC_OFF = PAGE_ID_OFF + 8;

    /** */
    public static final int COMMON_HEADER_END = 32; // type(2) + ver(2) + pageId(8) + crc(4) + reserved(16)

    /* All the page types. */

    /** */
    public static final short T_DATA = 1;

    /** */
    public static final short T_BPLUS_REF3_INNER = 2;

    /** */
    public static final short T_BPLUS_REF3_LEAF = 3;

    /** */
    public static final short  T_BPLUS_REF3_META = 4;

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
        assert type >= T_DATA && type <= T_BPLUS_REF3_META : type;

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
     * @return Type.
     */
    public abstract int getType();

    /**
     * @return Version.
     */
    public abstract int getVersion();

    /**
     * @param buf Buffer.
     * @param pageId Page ID.
     */
    public void initNewPage(ByteBuffer buf, long pageId) {
        setType(buf, getType());
        setVersion(buf, getVersion());
        setPageId(buf, pageId);
    }
}
