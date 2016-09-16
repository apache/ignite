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

package org.apache.ignite.internal.processors.hadoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * Common Hadoop utility methods which do not depend on Hadoop API.
 */
public class HadoopCommonUtils {
    /**
     * Sort input splits by length.
     *
     * @param splits Splits.
     * @return Sorted splits.
     */
    public static List<HadoopInputSplit> sortInputSplits(Collection<HadoopInputSplit> splits) {
        int id = 0;

        TreeSet<SplitSortWrapper> sortedSplits = new TreeSet<>();

        for (HadoopInputSplit split : splits) {
            long len = split instanceof HadoopFileBlock ? ((HadoopFileBlock)split).length() : 0;

            sortedSplits.add(new SplitSortWrapper(id++, split, len));
        }

        ArrayList<HadoopInputSplit> res = new ArrayList<>(sortedSplits.size());

        for (SplitSortWrapper sortedSplit : sortedSplits)
            res.add(sortedSplit.split);

        return res;
    }

    /**
     * Split wrapper for sorting.
     */
    private static class SplitSortWrapper implements Comparable<SplitSortWrapper> {
        /** Unique ID. */
        private final int id;

        /** Split. */
        private final HadoopInputSplit split;

        /** Split length. */
        private final long len;

        /**
         * Constructor.
         *
         * @param id Unique ID.
         * @param split Split.
         * @param len Split length.
         */
        public SplitSortWrapper(int id, HadoopInputSplit split, long len) {
            this.id = id;
            this.split = split;
            this.len = len;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(SplitSortWrapper other) {
            long res = len - other.len;

            if (res > 0)
                return -1;
            else if (res < 0)
                return 1;
            else
                return id - other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SplitSortWrapper && id == ((SplitSortWrapper)obj).id;
        }
    }

    /**
     * Private constructor.
     */
    private HadoopCommonUtils() {
        // No-op.
    }
}
