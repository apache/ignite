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
package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import org.apache.ignite.internal.pagemem.FullPageId;

/**
 * Class to collect several sets for same bucket (stripe), avoiding collection data copy. Class is not thread safe.
 */
public class MultiSetForSameStripeCollector {
    /**
     * Not merged sets for same bucket, but from different segments.
     */
    private Collection<Collection<FullPageId>> unmergedSets = new ArrayList<>(CheckpointScope.EXPECTED_SEGMENTS_COUNT);

    /**
     * @return overall size of contained sets.
     */
    public int size() {
        int size = 0;

        for (Collection<FullPageId> next : unmergedSets) {
            size += next.size();
        }

        return size;
    }

    /**
     * Appends next set to this collector, this method does not merge sets for performance reasons.
     * @param set data to add
     */
    public void add(Collection<FullPageId> set) {
        unmergedSets.add(set);
    }

    /**
     * @param comp comparator used for sorting collection.
     * @return Returns {@code true} if all sets are presorted using specific comparator instance.
     */
    public boolean isSorted(Comparator<FullPageId> comp) {
        for (Collection<FullPageId> next : unmergedSets) {
            if (!(next instanceof SortedSet))
                return false;

            SortedSet sortedSet = (SortedSet)next;

            if (sortedSet.comparator() != comp)
                return false;
        }
        return true;
    }

    public Iterable<? extends Collection<FullPageId>> unmergedSets() {
        return unmergedSets;
    }
}
