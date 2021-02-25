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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class helps to detect whether tree contains inlined JO type.
 *
 * When starting on old Ignite versions it's impossible to discover whether JO type was inlined or not.
 * Then try to find that with 2 steps:
 * 1. analyze of inline size;
 * 2. traverse tree and check stored values.
 */
public class InlineObjectBytesDetector implements BPlusTree.TreeRowClosure<H2Row, H2Row> {
    /** Inline size. */
    private final int inlineSize;

    /** Inline helpers. */
    private final List<InlineIndexColumn> inlineCols;

    /** Inline object supported flag. */
    private boolean inlineObjSupported = true;

    /** */
    private final String tblName;

    /** */
    private final String idxName;

    /** */
    private final IgniteLogger log;

    /**
     * @param inlineSize Inline size.
     * @param inlineCols Inline columns.
     * @param idxName Index name.
     * @param log Ignite logger.
     */
    InlineObjectBytesDetector(int inlineSize, List<InlineIndexColumn> inlineCols, String tblName, String idxName,
        IgniteLogger log) {
        this.inlineSize = inlineSize;
        this.inlineCols = inlineCols;
        this.tblName = tblName;
        this.idxName = idxName;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<H2Row, H2Row> tree, BPlusIO<H2Row> io,
        long pageAddr,
        int idx) throws IgniteCheckedException {
        H2Row r = tree.getRow(io, pageAddr, idx);

        int off = io.offset(idx);

        int fieldOff = 0;

        boolean varLenPresents = false;

        for (InlineIndexColumn ih : inlineCols) {
            if (fieldOff >= inlineSize)
                return false;

            if (ih.type() != Value.JAVA_OBJECT) {
                if (ih.size() < 0)
                    varLenPresents = true;

                fieldOff += ih.fullSize(pageAddr, off + fieldOff);

                continue;
            }

            Value val = r.getValue(ih.columnIndex());

            if (val == ValueNull.INSTANCE)
                return false;

            int type = PageUtils.getByte(pageAddr, off + fieldOff);

            // We can have garbage in memory and need to compare data.
            if (type == Value.JAVA_OBJECT) {
                int len = PageUtils.getShort(pageAddr, off + fieldOff + 1);

                len &= 0x7FFF;

                byte[] originalObjBytes = val.getBytesNoCopy();

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

            if (type == Value.UNKNOWN && varLenPresents) {
                // We can't guarantee in case unknown type and should check next row:
                // 1: long string, UNKNOWN for java object.
                // 2: short string, inlined java object
                return false;
            }

            inlineObjectSupportedDecision(false, "inline type " + type);

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
     * @param inlineCols Inline columns.
     * @param inlineSize Inline size.
     *
     * @return {@code true} If the object may be inlined.
     */
    public static boolean objectMayBeInlined(int inlineSize, List<InlineIndexColumn> inlineCols) {
        int remainSize = inlineSize;

        for (InlineIndexColumn ih : inlineCols) {
            if (ih.type() == Value.JAVA_OBJECT)
                break;

            // Set size to 1 for variable length columns as that value can be set by user.
            remainSize -= ih.size() > 0 ? 1 + ih.size() : 1;
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
            log.warning("Index supports JAVA_OBJECT type inlining [tblName=" + tblName + ", idxName=" +
                idxName + ", reason='" + reason + "']");
        else
            log.warning("Index doesn't support JAVA_OBJECT type inlining [tblName=" + tblName + ", idxName=" +
                idxName + ", reason='" + reason + "']");
    }
}
