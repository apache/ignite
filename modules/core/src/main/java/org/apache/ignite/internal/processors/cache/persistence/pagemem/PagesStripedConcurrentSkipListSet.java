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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;

/**
 * Special set to store page IDs in buckets based on cache Group ID + partition ID.
 * Stripes pages by value of Hash(cacheGroupId,partitionId) into several sets.
 * Inner set for buckets in this implementation is sorted (skip list) set.
 * Inner sets can be taken at checkpoint, and then merged into one sorted set for each stripe.
 * Using this implementation avoids sorting at checkpointer thread(s).
 */
public class PagesStripedConcurrentSkipListSet extends PagesStripedConcurrentHashSet {
    /**
     * @return created new set for bucket data storage.
     */
    @Override protected Set<FullPageId> createNewSet() {
        return new GridConcurrentSkipListSetEx<>(GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR);
    }

    /**
     * Provides overriden method {@code #size()}. NOTE: Only the following methods supports this addition: <ul>
     * <li>{@code #add()}</li> <li>{@code #remove()}</li> </ul>. Usage of other methods may make size inconsistent.
     */
    private static class GridConcurrentSkipListSetEx<E> extends GridConcurrentSkipListSet<E> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Size. */
        private final LongAdder size = new LongAdder();

        /**
         * @param comp Comparator.
         */
        public GridConcurrentSkipListSetEx(Comparator<E> comp) {
            super(comp);
        }

        /**
         * @return Size based on performed operations.
         */
        @Override public int size() {
            return size.intValue();
        }

        /** {@inheritDoc} */
        @Override public boolean add(E e) {
            boolean res = super.add(e);

            if (res)
                size.increment();

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            boolean res = super.remove(o);

            if (res)
                size.decrement();

            return res;
        }
    }
}
