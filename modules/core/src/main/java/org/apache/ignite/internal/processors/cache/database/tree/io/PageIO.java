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
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + "[ver=" + getVersion() + "]";
    }
}
