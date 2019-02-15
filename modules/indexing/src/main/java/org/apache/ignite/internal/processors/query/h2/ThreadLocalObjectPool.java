/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Special pool for managing limited number objects for further reuse.
 * This pool maintains separate object bag for each thread by means of {@link ThreadLocal}.
 * <p>
 * If object is borrowed on one thread and recycled on different then it will be returned to
 * recycling thread bag. For thread-safe use either pooled objects should be thread-safe or
 * <i>happens-before</i> should be established between borrowing object and subsequent recycling.
 *
 * @param <E> pooled objects type
 */
public final class ThreadLocalObjectPool<E extends AutoCloseable> {
    /**
     * Wrapper for a pooled object with capability to return the object to a pool.
     *
     * @param <T> enclosed object type
     */
    public static class Reusable<T extends AutoCloseable> {
        /** */
        private final ThreadLocalObjectPool<T> pool;
        /** */
        private final T object;

        /** */
        private Reusable(ThreadLocalObjectPool<T> pool, T object) {
            this.pool = pool;
            this.object = object;
        }

        /**
         * @return enclosed object
         */
        public T object() {
            return object;
        }

        /**
         * Returns an object to a pool or closes it if the pool is already full.
         */
        public void recycle() {
            Queue<Reusable<T>> bag = pool.bag.get();
            if (bag.size() < pool.poolSize)
                bag.add(this);
            else
                U.closeQuiet(object);
        }
    }

    /** */
    private final Supplier<E> objectFactory;
    /** */
    private final ThreadLocal<Queue<Reusable<E>>> bag = ThreadLocal.withInitial(LinkedList::new);
    /** */
    private final int poolSize;

    /**
     * @param objectFactory factory used for new objects creation
     * @param poolSize number of objects which pool can contain
     */
    public ThreadLocalObjectPool(Supplier<E> objectFactory, int poolSize) {
        this.objectFactory = objectFactory;
        this.poolSize = poolSize;
    }

    /**
     * Picks an object from the pool if one is present or creates new one otherwise.
     * Returns an object wrapper which could be returned to the pool.
     *
     * @return reusable object wrapper
     */
    public Reusable<E> borrow() {
        Reusable<E> pooled = bag.get().poll();
        return pooled != null ? pooled : new Reusable<>(this, objectFactory.get());
    }

    /** Visible for test */
    int bagSize() {
        return bag.get().size();
    }
}
