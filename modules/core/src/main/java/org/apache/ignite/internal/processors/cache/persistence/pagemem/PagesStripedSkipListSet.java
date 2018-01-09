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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.jsr166.LongAdder8;

/**
 *
 */
public class PagesStripedSkipListSet extends PagesConcurrentHashSet {
    /**
     * @return created new set for bucket data storage.
     */
    @Override protected Set<FullPageId> createNewSet() {
        return new GridConcurrentSkipListSetEx<>(GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR);
    }

    /**
     * Provides overriden method {@code #size ()}. NOTE: Only the following methods supports this addition: <ul>
     * <li>{@code #add()}</li> <li>{@code #remove()}</li> <ul/>
     */
    private static class GridConcurrentSkipListSetEx<E> extends GridConcurrentSkipListSet<E> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Size. */
        private final LongAdder8 size = new LongAdder8();

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
