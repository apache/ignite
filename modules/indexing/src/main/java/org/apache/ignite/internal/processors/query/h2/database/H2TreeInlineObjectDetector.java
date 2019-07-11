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

    /**
     * @param inlineSize Inline size.
     * @param inlineHelpers Inline helpers.
     */
    H2TreeInlineObjectDetector(int inlineSize, List<InlineIndexHelper> inlineHelpers) {
        this.inlineSize = inlineSize;
        this.inlineHelpers = inlineHelpers;
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

            if (r.getValue(ih.columnIndex()) == ValueNull.INSTANCE)
                return false;

            int type = PageUtils.getByte(pageAddr, off + fieldOff);

            if (type == 0) {
                inlineObjSupported = false;

                return true;
            }
            else if (type == Value.JAVA_OBJECT) {
                inlineObjSupported = true;

                return true;
            }
            else {
                assert type == Value.UNKNOWN;

                return false;
            }
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
}
