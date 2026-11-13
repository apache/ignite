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

package org.apache.ignite.internal.util.tostring;

import java.util.function.Supplier;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.COLLECTION_LIMIT;

/**
 * Appends a "skipped elements" hint to a string builder
 * when a size exceeds the limit.
 */
class LongSequenceSkipRule {
    /** The number of elements that were skipped due to the size limit.*/
    private final int skipped;

    /**
     * Constructor.
     * Calculates the number of skipped elements based on size and limit.
     * @param sizeSupplier Supplier that provides the actual size.
     */
    LongSequenceSkipRule(Supplier<Integer> sizeSupplier) {
        skipped = Math.max(0, sizeSupplier.get() - COLLECTION_LIMIT);
    }

    /**
     * Appends a hint about skipped elements to the string builder
     * if any were skipped.
     * @param sb The string builder to append the hint to.
     */
    void appendSkippedCountHint(GridStringBuilder sb) {
        if (skipped != 0)
            sb.a("... and ").a(skipped).a(" more");
    }
}
