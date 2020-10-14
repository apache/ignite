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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class InlineRecommender {
    /** @see #IGNITE_THROTTLE_INLINE_SIZE_CALCULATION */
    public static final int DFLT_THROTTLE_INLINE_SIZE_CALCULATION = 1_000;

    /** */
    @SystemProperty(value = "How often real invocation of inline size calculation will be skipped.", type = Long.class,
        defaults = "" + DFLT_THROTTLE_INLINE_SIZE_CALCULATION)
    public static final String IGNITE_THROTTLE_INLINE_SIZE_CALCULATION = "IGNITE_THROTTLE_INLINE_SIZE_CALCULATION";

    /** Counter of inline size calculation for throttling real invocations. */
    private final ThreadLocal<Long> inlineSizeCalculationCntr = ThreadLocal.withInitial(() -> 0L);

    /** How often real invocation of inline size calculation will be skipped. */
    private final int inlineSizeThrottleThreshold =
        IgniteSystemProperties.getInteger(IGNITE_THROTTLE_INLINE_SIZE_CALCULATION,
            DFLT_THROTTLE_INLINE_SIZE_CALCULATION);

    /** Keep max calculated inline size for current index. */
    // TODO: share it between segments.
    private final AtomicInteger maxCalculatedInlineSize = new AtomicInteger();

    private final IgniteLogger log;

    private final String cacheName;

    private final String idxName;

    public InlineRecommender(IgniteLogger log, SortedIndexDefinition def) {
        cacheName = def.getContext().name();
        idxName = def.getIdxName();
        this.log = log;
    }

    /**
     * Calculate aggregate inline size for given indexes and log recommendation in case calculated size more than
     * current inline size.
     */
    @SuppressWarnings({"ConditionalBreakInInfiniteLoop", "IfMayBeConditional"})
    public void recommend(IndexRow row, int currInlineSize) {
        //TODO Do the check only for put operations.
//        if (!(row instanceof H2CacheRow))
//            return;

        Long invokeCnt = inlineSizeCalculationCntr.get();

        inlineSizeCalculationCntr.set(++invokeCnt);

        boolean throttle = invokeCnt % inlineSizeThrottleThreshold != 0;

        if (throttle)
            return;

        int newSize = 0;

        for (int i = 0; i < row.getSchema().getInlineKeys().length; i++)
            newSize += row.getSchema().getInlineKeys()[i].getInlineType().inlineSize(row.getKey(i));

        if (newSize > currInlineSize) {
            int oldSize;

            while (true) {
                oldSize = maxCalculatedInlineSize.get();

                if (oldSize >= newSize)
                    return;

                if (maxCalculatedInlineSize.compareAndSet(oldSize, newSize))
                    break;
            }

//            String cols = colNames.stream().collect(Collectors.joining(", ", "(", ")"));
//
//            String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";
            String idxType = "SECONDARY";

            String recommendation;

            // TODO
//            if (pk || affinityKey) {
//                recommendation = "set system property "
//                    + IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE + " with recommended size " +
//                    "(be aware it will be used by default for all indexes without explicit inline size)";
//            }
//            else {
                recommendation = "use INLINE_SIZE option for CREATE INDEX command, " +
                    "QuerySqlField.inlineSize for annotated classes, or QueryIndex.inlineSize for explicit " +
                    "QueryEntity configuration";
//            }

            String warn = "Indexed columns of a row cannot be fully inlined into index " +
                "what may lead to slowdown due to additional data page reads, increase index inline size if needed " +
                "(" + recommendation + ") " +
                "[cacheName=" + cacheName +
                ", idxName=" + idxName +
                // TODO: cols for query definition?
//                ", idxCols=" + idxCols +
                ", idxType=" + idxType +
                ", curSize=" + currInlineSize +
                ", recommendedInlineSize=" + newSize + "]";

            U.warn(log, warn);
        }
    }
}
