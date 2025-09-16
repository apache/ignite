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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THROTTLE_INLINE_SIZE_CALCULATION;

/**
 * Write to a log recommendation for inline size.
 */
public class InlineRecommender {
    /** Default throttle frequency for an index row inline size calculation and logging index inline size recommendation. */
    public static final int DFLT_THROTTLE_INLINE_SIZE_CALCULATION = 1_000;

    /** Recommended number of child items to avoid index performance drop / tree degeneration. */
    private static final int RECOMMENDED_CHILD_NUMBER = 2;

    /** Counter of inline size calculation for throttling real invocations. */
    private final AtomicLong inlineSizeCalculationCntr = new AtomicLong();

    /** How often real invocation of inline size calculation will be skipped. */
    private final int inlineSizeThrottleThreshold =
        IgniteSystemProperties.getInteger(IGNITE_THROTTLE_INLINE_SIZE_CALCULATION,
            DFLT_THROTTLE_INLINE_SIZE_CALCULATION);

    /** Keep max calculated inline size for current index. */
    private final AtomicInteger maxCalculatedInlineSize = new AtomicInteger();

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Index definition. */
    private final SortedIndexDefinition def;

    /** Constructor. */
    public InlineRecommender(GridCacheContext<?, ?> cctx, SortedIndexDefinition def) {
        log = cctx.kernalContext().indexProcessor().logger();
        this.def = def;
    }

    /**
     * Calculate aggregate inline size for given indexes and log recommendation in case calculated size more than
     * current inline size.
     */
    @SuppressWarnings({"ConditionalBreakInInfiniteLoop", "IfMayBeConditional"})
    public void recommend(IndexRow row, int currInlineSize, int pageSize) {
        // Do the check only for put operations.
        if (row.indexPlainRow())
            return;

        long invokeCnt = inlineSizeCalculationCntr.get();

        if (!inlineSizeCalculationCntr.compareAndSet(invokeCnt, invokeCnt + 1))
            return;

        boolean throttle = invokeCnt + 1 % inlineSizeThrottleThreshold != 0;

        if (throttle)
            return;

        int maxRecommendedInlineSize = maxRecommendedInlineSize(pageSize);

        if (currInlineSize > maxRecommendedInlineSize) {
            U.warn(log, "Inline size is too big and may lead to performance degradation or tree degeneration. " +
                    "Consider decreasing inline size or increasing page size " + "(" + getRecommendation() + ") " +
                    "[cacheName=" + def.idxName().cacheName() +
                    ", tableName=" + def.idxName().tableName() +
                    ", idxName=" + def.idxName().idxName() +
                    ", inlineSize=" + currInlineSize +
                    ", pageSize=" + pageSize +
                    ", maxRecommendedInlineSize=" + maxRecommendedInlineSize + "]");

            return;
        }

        int newSize = 0;

        for (int i = 0; i < row.rowHandler().inlineIndexKeyTypes().size(); i++) {
            InlineIndexKeyType keyType = row.rowHandler().inlineIndexKeyTypes().get(i);

            newSize += keyType.inlineSize(row.key(i));
        }

        if (newSize > currInlineSize) {
            int oldSize;

            while (true) {
                oldSize = maxCalculatedInlineSize.get();

                if (oldSize >= newSize)
                    return;

                if (maxCalculatedInlineSize.compareAndSet(oldSize, newSize))
                    break;
            }

            String cols = def.indexKeyDefinitions().keySet().stream()
                .collect(Collectors.joining(", ", "(", ")"));

            String type = def.primary() ? "PRIMARY KEY" : def.affinity() ? "AFFINITY KEY (implicit)" : "SECONDARY";

            String warn = "Indexed columns of a row cannot be fully inlined into index " +
                "what may lead to slowdown due to additional data page reads, increase index inline size if needed " +
                "(" + getRecommendation() + ") " +
                "[cacheName=" + def.idxName().cacheName() +
                ", tableName=" + def.idxName().tableName() +
                ", idxName=" + def.idxName().idxName() +
                ", idxCols=" + cols +
                ", idxType=" + type +
                ", curSize=" + currInlineSize +
                ", recommendedInlineSize=" + newSize + "]";

            U.warn(log, warn);
        }
    }

    /** Returns a recommendation how to fix the inline size issue. */
    private String getRecommendation() {
        if (def.primary() || def.affinity()) {
            return "set system property "
                + IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE + " with recommended size " +
                "(be aware it will be used by default for all indexes without explicit inline size)";
        }
        else {
            return "use INLINE_SIZE option for CREATE INDEX command, " +
                "QuerySqlField.inlineSize for annotated classes, or QueryIndex.inlineSize for explicit " +
                "QueryEntity configuration";
        }
    }

    /**
     * To avoid performance problems (i.e. tree degeneration), at least {@link #RECOMMENDED_CHILD_NUMBER} items should
     * fit into one page. So maximum inline size equals: I = (PS - H - (N + 1) * L) / N - R, where I - inline size,
     * PS - page size, H - page header size, L - size of the child link, P - number of child elements, R - row link size.
     */
    private int maxRecommendedInlineSize(int pageSize) {
        return (pageSize - AbstractDataPageIO.ITEMS_OFF - (RECOMMENDED_CHILD_NUMBER + 1) * AbstractDataPageIO.LINK_SIZE)
                / RECOMMENDED_CHILD_NUMBER - Long.BYTES;
    }
}
