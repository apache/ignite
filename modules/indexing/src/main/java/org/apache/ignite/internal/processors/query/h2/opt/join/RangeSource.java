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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;

import static java.util.Collections.emptyIterator;

/**
 * Bounds iterator.
 */
public class RangeSource {
    /** Index. */
    private final H2TreeIndex idx;

    /** */
    private Iterator<GridH2RowRangeBounds> boundsIter;

    /** */
    private int curRangeId = -1;

    /** */
    private final int segment;

    /** */
    private final BPlusTree.TreeRowClosure<H2Row, H2Row> filter;

    /** Iterator. */
    private Iterator<H2Row> iter = emptyIterator();

    /**
     * @param bounds Bounds.
     * @param segment Segment.
     * @param filter Filter.
     */
    public RangeSource(
        H2TreeIndex idx,
        Iterable<GridH2RowRangeBounds> bounds,
        int segment,
        BPlusTree.TreeRowClosure<H2Row, H2Row> filter
    ) {
        this.idx = idx;
        this.segment = segment;
        this.filter = filter;

        boundsIter = bounds.iterator();
    }

    /**
     * @return {@code true} If there are more rows in this source.
     */
    public boolean hasMoreRows() {
        return boundsIter.hasNext() || iter.hasNext();
    }

    /**
     * @param maxRows Max allowed rows.
     * @return Range.
     */
    public GridH2RowRange next(int maxRows) {
        assert maxRows > 0 : maxRows;

        for (; ; ) {
            if (iter.hasNext()) {
                // Here we are getting last rows from previously partially fetched range.
                List<GridH2RowMessage> rows = new ArrayList<>();

                GridH2RowRange nextRange = new GridH2RowRange();

                nextRange.rangeId(curRangeId);
                nextRange.rows(rows);

                do {
                    rows.add(H2Utils.toRowMessage(iter.next()));
                }
                while (rows.size() < maxRows && iter.hasNext());

                if (iter.hasNext())
                    nextRange.setPartial();
                else
                    iter = emptyIterator();

                return nextRange;
            }

            iter = emptyIterator();

            if (!boundsIter.hasNext()) {
                boundsIter = emptyIterator();

                return null;
            }

            GridH2RowRangeBounds bounds = boundsIter.next();

            curRangeId = bounds.rangeId();

            iter = idx.findForSegment(bounds, segment, filter);

            if (!iter.hasNext()) {
                // We have to return empty range here.
                GridH2RowRange emptyRange = new GridH2RowRange();

                emptyRange.rangeId(curRangeId);

                return emptyRange;
            }
        }
    }
}
