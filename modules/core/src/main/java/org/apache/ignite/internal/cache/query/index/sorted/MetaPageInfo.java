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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Meta page stores meta data about InlineIndexTree.
 */
public class MetaPageInfo {
    /** Inline size used for a tree. */
    private final int inlineSize;

    /** Whether index use wrapped / unwrapped PK. */
    private final boolean useUnwrappedPk;

    /** Whether any flags is supported. */
    private final boolean flagsSupported;

    /** Whether inlinining of java objects is supported. */
    private boolean inlineObjSupported;

    /** Whether inlinining of java objects as hash is supported. */
    private boolean inlineObjHash;

    /** Version of Ignite. */
    private final IgniteProductVersion createdVer;

    /**
     * @param io Metapage IO.
     * @param pageAddr Page address.
     */
    public MetaPageInfo(BPlusMetaIO io, long pageAddr) {
        inlineSize = io.getInlineSize(pageAddr);
        useUnwrappedPk = io.unwrappedPk(pageAddr);
        flagsSupported = io.supportFlags();

        if (flagsSupported) {
            inlineObjSupported = io.inlineObjectSupported(pageAddr);
            inlineObjHash = io.inlineObjectHash(pageAddr);
        }

        createdVer = io.createdVersion(pageAddr);
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @return {@code true} In case use unwrapped PK for indexes.
     */
    public boolean useUnwrappedPk() {
        return useUnwrappedPk;
    }

    /**
     * @return {@code true} In case metapage contains flags.
     */
    public boolean flagsSupported() {
        return flagsSupported;
    }

    /**
     * @return {@code true} In case inline object is supported.
     */
    public boolean inlineObjectSupported() {
        return inlineObjSupported;
    }

    /**
     * @return {@code true} In case inline object is supported.
     */
    public boolean inlineObjectHash() {
        return inlineObjHash;
    }

    /**
     * Reads meta page info from page memory.
     *
     * @param metaPageId Meta page ID.
     * @param grpId Cache group ID.
     * @param pageMemory Page memory.
     * @return Meta page info.
     * @throws IgniteCheckedException If something went wrong.
     */
    public static MetaPageInfo read(long metaPageId, int grpId, PageMemory pageMemory) throws IgniteCheckedException {
        final long metaPage = pageMemory.acquirePage(grpId, metaPageId);

        try {
            long pageAddr = pageMemory.readLock(grpId, metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" + U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return new MetaPageInfo(io, pageAddr);
            }
            finally {
                pageMemory.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            pageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }

    /**
     * Writes meta page info into page memory.
     *
     * @param metaPageId Meta page ID.
     * @param grpId Cache group ID.
     * @param pageMemory Page memory.
     * @throws IgniteCheckedException If something went wrong.
     */
    public void write(long metaPageId, int grpId, PageMemory pageMemory) throws IgniteCheckedException {
        final long metaPage = pageMemory.acquirePage(grpId, metaPageId);

        try {
            long pageAddr = pageMemory.writeLock(grpId, metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to write lock meta page [metaPageId=" + U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.setValues(
                    pageAddr,
                    inlineSize(),
                    useUnwrappedPk(),
                    inlineObjectSupported(),
                    inlineObjectHash()
                );
            }
            finally {
                pageMemory.writeUnlock(grpId, metaPageId, metaPage, null, true);
            }
        }
        finally {
            pageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }
}
