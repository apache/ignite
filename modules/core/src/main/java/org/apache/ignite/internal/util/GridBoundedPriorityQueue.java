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

package org.apache.ignite.internal.util;

import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Bounded variant of {@link PriorityQueue}.
 *
 * @param <E> Type of the queue element.
 */
public class GridBoundedPriorityQueue<E> extends PriorityQueue<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Queue max capacity. */
    private final int maxCap;

    /** Comparator. */
    private final Comparator<? super E> cmp;

    /**
     * Creates a bounded priority queue with the specified maximum size.
     * At most {@code maxCap} elements would be kept in the queue.
     *
     * @param maxCap Maximum size of the queue.
     * @param cmp Comparator that orders the elements.
     */
    public GridBoundedPriorityQueue(int maxCap, Comparator<? super E> cmp) {
        super(maxCap, cmp);

        A.notNull(cmp, "comparator not null");

        this.maxCap = maxCap;
        this.cmp = cmp;
    }

    /** {@inheritDoc} */
    @Override public boolean offer(E e) {
        if (size() >= maxCap) {
            E head = peek();

            if (cmp.compare(e, head) <= 0)
                return false;

            poll();
        }

        return super.offer(e);
    }
}
