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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.JavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;

/**
 * This class helps to detect whether tree contains inlined JO type.
 *
 * When starting on old Ignite versions it's impossible to discover whether JO type was inlined or not.
 * Then try to find that with 2 steps:
 * 1. analyze of inline size;
 * 2. traverse tree and check stored values.
 */
public class InlineObjectBytesDetector implements BPlusTree.TreeRowClosure<IndexRow, IndexRow> {
    /** Inline size. */
    private final int inlineSize;

    /** Inline helpers. */
    private final Collection<IndexKeyDefinition> keyDefs;

    /** Inline object supported flag. */
    private boolean inlineObjSupported = true;

    /** */
    private final IndexName idxName;

    /** */
    private final IgniteLogger log;

    /**
     * @param inlineSize Inline size.
     * @param keyDefs Index key definitions.
     * @param idxName Index name.
     * @param log Ignite logger.
     */
    public InlineObjectBytesDetector(int inlineSize, Collection<IndexKeyDefinition> keyDefs, IndexName idxName,
        IgniteLogger log) {
        this.inlineSize = inlineSize;
        this.keyDefs = keyDefs;
        this.idxName = idxName;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<IndexRow, IndexRow> tree, BPlusIO<IndexRow> io,
        long pageAddr,
        int idx) throws IgniteCheckedException {
        IndexRow r = tree.getRow(io, pageAddr, idx);

        int off = io.offset(idx);

        int fieldOff = 0;

        boolean varLenPresents = false;

        IndexKeyTypeSettings keyTypeSettings = new IndexKeyTypeSettings();

        Iterator<IndexKeyDefinition> it = keyDefs.iterator();

        for (int i = 0; i < keyDefs.size(); ++i) {
            IndexKeyDefinition keyDef = it.next();

            if (fieldOff >= inlineSize)
                return false;

            if (keyDef.idxType() != IndexKeyType.JAVA_OBJECT) {
                InlineIndexKeyType keyType = InlineIndexKeyTypeRegistry.get(keyDef.idxType(), keyTypeSettings);

                if (keyType.inlineSize() < 0)
                    varLenPresents = true;

                fieldOff += keyType.inlineSize(pageAddr, off + fieldOff);

                continue;
            }

            IndexKey key = r.key(i);

            if (key == NullIndexKey.INSTANCE)
                return false;

            int typeCode = PageUtils.getByte(pageAddr, off + fieldOff);

            // We can have garbage in memory and need to compare data.
            if (typeCode == IndexKeyType.JAVA_OBJECT.code()) {
                int len = PageUtils.getShort(pageAddr, off + fieldOff + 1);

                len &= 0x7FFF;

                byte[] originalObjBytes = ((JavaObjectIndexKey)key).bytesNoCopy();

                // Read size more then available space or more then origin length.
                if (len > inlineSize - fieldOff - 3 || len > originalObjBytes.length) {
                    inlineObjectSupportedDecision(false, "length is big " + len);

                    return true;
                }

                // Try compare byte by byte for fully or partial inlined object.
                byte[] inlineBytes = PageUtils.getBytes(pageAddr, off + fieldOff + 3, len);

                if (!Arrays.equals(inlineBytes, originalObjBytes)) {
                    inlineObjectSupportedDecision(false, "byte compare");

                    return true;
                }

                inlineObjectSupportedDecision(true, len + " bytes compared");

                return true;
            }

            if (typeCode == IndexKeyType.UNKNOWN.code() && varLenPresents) {
                // We can't guarantee in case unknown type and should check next row:
                // 1: long string, UNKNOWN for java object.
                // 2: short string, inlined java object
                return false;
            }

            inlineObjectSupportedDecision(false, "inline type " + typeCode);

            return true;
        }

        inlineObjectSupportedDecision(true, "no java objects for inlining");

        return true;
    }

    /**
     * @return {@code true} if inline object is supported on current tree.
     */
    public boolean inlineObjectSupported() {
        return inlineObjSupported;
    }

    /**
     * Static analyze inline_size and inline columns set.
     * e.g.: indexed: (long, obj) and inline_size < 12.
     * In this case there is no space for inline object.
     *
     * @param keyDefs Index key definition.
     * @param inlineSize Inline size.
     *
     * @return {@code true} If the object may be inlined.
     */
    public static boolean objectMayBeInlined(int inlineSize, Collection<IndexKeyDefinition> keyDefs) {
        int remainSize = inlineSize;

        // The settings does not affect on inline size.
        IndexKeyTypeSettings settings = new IndexKeyTypeSettings();

        for (IndexKeyDefinition def: keyDefs) {
            if (def.idxType() == IndexKeyType.JAVA_OBJECT)
                break;

            InlineIndexKeyType keyType = InlineIndexKeyTypeRegistry.get(def.idxType(), settings);

            if (keyType == null)
                return false;

            // Set size to 1 for variable length columns as that value can be set by user.
            remainSize -= keyType.inlineSize() > 0 ? 1 + keyType.inlineSize() : 1;
        }

        // For old versions JO type was inlined as byte array.
        return remainSize >= 4;
    }

    /**
     * @param inlineObjSupported {@code true} if inline object is supported on current tree.
     * @param reason Reason why has been made decision.
     */
    private void inlineObjectSupportedDecision(boolean inlineObjSupported, String reason) {
        this.inlineObjSupported = inlineObjSupported;

        if (inlineObjSupported)
            log.warning("Index supports JAVA_OBJECT type inlining [tblName=" + idxName.tableName() + ", idxName=" +
                idxName + ", reason='" + reason + "']");
        else
            log.warning("Index doesn't support JAVA_OBJECT type inlining [tblName=" + idxName.tableName() + ", idxName=" +
                idxName + ", reason='" + reason + "']");
    }
}
