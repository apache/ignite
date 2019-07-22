/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Used to detect inline object. Because version wasn't upped.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class H2TreeInlineObjectDetector implements BPlusTree.TreeRowClosure<SearchRow, GridH2Row> {
    /** Inline size. */
    private final int inlineSize;

    /** Inline helpers. */
    private final List<InlineIndexHelper> inlineHelpers;

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
     * @param inlineHelpers Inline helpers.
     * @param tblName Table name.
     * @param idxName Name of index.
     * @param log Logger.
     */
    H2TreeInlineObjectDetector(int inlineSize, List<InlineIndexHelper> inlineHelpers, String tblName, String idxName,
        IgniteLogger log) {
        this.inlineSize = inlineSize;
        this.inlineHelpers = inlineHelpers;
        this.tblName = tblName;
        this.idxName = idxName;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<SearchRow, GridH2Row> tree, BPlusIO<SearchRow> io, long pageAddr,
        int idx) throws IgniteCheckedException {
        GridH2Row r = tree.getRow(io, pageAddr, idx);

        int off = io.offset(idx);

        int fieldOff = 0;

        for (InlineIndexHelper ih : inlineHelpers) {
            if (fieldOff >= inlineSize)
                return false;

            if (ih.type() != Value.JAVA_OBJECT) {
                fieldOff += ih.fullSize(pageAddr, off + fieldOff);

                continue;
            }

            Value val = r.getValue(ih.columnIndex());

            if (val == ValueNull.INSTANCE)
                return false;

            int type = PageUtils.getByte(pageAddr, off + fieldOff);

            //We can have garbage in memory and need to compare data.
            if (type == Value.JAVA_OBJECT) {
                int len = PageUtils.getShort(pageAddr, off + fieldOff + 1);

                boolean isFull = (len & 0x8000) == 0;

                len = len & 0x7FFF;

                byte[] originalObjBytes = val.getBytesNoCopy();

                // read size more then available space or more then origin length
                if(len > inlineSize - fieldOff - 3 || len > originalObjBytes.length){

                    inlineObjectSupportedDecision(false, "lenght is big " + len);

                    return true;
                }

                // compare full object
                if(isFull) {
                    int compRes = ih.compare(pageAddr, off + fieldOff, inlineSize - fieldOff, val, (o1, o2) -> 0);

                    inlineObjectSupportedDecision(compRes == 0, "full compare");

                    return true;
                }


                // try compare byte by byte for partial inlined object.
                byte[] inlineBytes = PageUtils.getBytes(pageAddr, off + fieldOff + 3, len);

                for (int i = 0; i < len; i++) {
                    if(inlineBytes[i] == originalObjBytes[i])
                        continue;

                    inlineObjectSupportedDecision(false,  i + " byte compare");

                    return true;
                }

                inlineObjectSupportedDecision(true, len + " bytes compared");

                return true;
            }

            inlineObjectSupportedDecision(false, "inline type " + type);

            return true;
        }

        return true;
    }

    /**
     * @return {@code true} if inline object is supported on current tree.
     */
    public boolean inlineObjectSupported() {
        return inlineObjSupported;
    }

    /**
     * Static analyze inline_size and inline helpers set.
     * e.g.: indexed: (long, obj) and inline_size < 12.
     * In this case there is no space ti inline object.
     *
     * @param inlineHelpers Inline helpers.
     * @param inlineSize Inline size.
     *
     * @return {@code true} If the object may be inlined.
     */
    public static boolean objectMayBeInlined(int inlineSize, List<InlineIndexHelper> inlineHelpers) {
        int remainSize = inlineSize;

        for (InlineIndexHelper ih : inlineHelpers) {
            if (ih.type() == Value.JAVA_OBJECT)
                break;

            remainSize -= ih.size() > 0 ? 1 + ih.size() : 4;
        }

        return remainSize >= 4;
    }

    /**
     * @param inlineObjSupported {@code true} if inline object is supported on current tree.
     * @param reason Reason why has been made decision.
     */
    private void inlineObjectSupportedDecision(boolean inlineObjSupported, String reason){
        this.inlineObjSupported = inlineObjSupported;

        if (inlineObjSupported)
            log.info("Index support JAVA_OBJECT type inlining [tblName=" + tblName +", idxName=" +
                idxName + ", reason='" + reason + "']");
        else
            log.info("Index doesn't support JAVA_OBJECT type inlining [tblName=" + tblName +", idxName=" +
                idxName + ", reason='" + reason + "']");
    }
}
