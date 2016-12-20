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
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * IO routines for B+Tree meta pages.
 */
public class BPlusMetaIO extends PageIO {
    public static final BPlusMetaIO VERSION1 = new BPlusMetaIO(1);

    /** */
    public static final IOVersions<BPlusMetaIO> VERSIONS = new IOVersions<>(
        VERSION1
    );

    /** */
    private static final int LVLS_OFF = COMMON_HEADER_END;

    /** */
    private static final int REFS_OFF = LVLS_OFF + 1;

    /**
     * @param ver Page format version.
     */
    protected BPlusMetaIO(int ver) {
        super(T_BPLUS_META, ver);
    }

    /**
     * @param buf Buffer.
     * @param rootId Root page ID.
     */
    public void initRoot(ByteBuffer buf, long rootId) {
        setLevelsCount(buf, 1);
        setFirstPageId(buf, 0, rootId);
    }

    /**
     * @param buf Buffer.
     * @return Number of levels in this tree.
     */
    public int getLevelsCount(ByteBuffer buf) {
        return buf.get(LVLS_OFF);
    }

    public int getLevelsCount(long buf) {
        return GridUnsafe.getByte(buf, LVLS_OFF);
    }

    /**
     * @param buf Buffer.
     * @return Max levels possible for this page size.
     */
    public int getMaxLevels(ByteBuffer buf) {
        return (buf.capacity() - REFS_OFF) / 8;
    }

    /**
     * @param buf  Buffer.
     * @param lvls Number of levels in this tree.
     */
    public void setLevelsCount(ByteBuffer buf, int lvls) {
        assert lvls >= 0 && lvls <= getMaxLevels(buf): lvls;

        buf.put(LVLS_OFF, (byte)lvls);

        assert getLevelsCount(buf) == lvls;
    }

    /**
     * @param lvl Level.
     * @return Offset for page reference.
     */
    private static int offset(int lvl) {
        return lvl * 8 + REFS_OFF;
    }

    /**
     * @param buf Buffer.
     * @param lvl Level.
     * @return First page ID at that level.
     */
    public long getFirstPageId(ByteBuffer buf, int lvl) {
        return buf.getLong(offset(lvl));
    }

    public long getFirstPageId(long buf, int lvl) {
        return GridUnsafe.getLong(buf, offset(lvl));
    }

    /**
     * @param buf    Buffer.
     * @param lvl    Level.
     * @param pageId Page ID.
     */
    public void setFirstPageId(ByteBuffer buf, int lvl, long pageId) {
        assert lvl >= 0 && lvl < getLevelsCount(buf);

        buf.putLong(offset(lvl), pageId);

        assert getFirstPageId(buf, lvl) == pageId;
    }

    /**
     * @param buf Buffer.
     * @return Root level.
     */
    public int getRootLevel(ByteBuffer buf) {
        int lvls = getLevelsCount(buf); // The highest level page is root.

        assert lvls > 0 : lvls;

        return lvls - 1;
    }

    public int getRootLevel(long buf) {
        int lvls = getLevelsCount(buf); // The highest level page is root.

        assert lvls > 0 : lvls;

        return lvls - 1;
    }

    /**
     * @param buf Buffer.
     * @param rootPageId New root page ID.
     */
    public void addRoot(ByteBuffer buf, long rootPageId) {
        int lvl = getLevelsCount(buf);

        setLevelsCount(buf, lvl + 1);
        setFirstPageId(buf, lvl, rootPageId);
    }

    /**
     * @param buf Buffer.
     */
    public void cutRoot(ByteBuffer buf) {
        int lvl = getRootLevel(buf);

        setLevelsCount(buf, lvl); // Decrease tree height.
    }
}
