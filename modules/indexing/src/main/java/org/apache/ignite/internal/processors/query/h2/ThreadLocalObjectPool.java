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

package org.apache.ignite.internal.processors.query.h2;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;
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
    /** */
    private final ThreadLocal<Queue<E>> bag = ThreadLocal.withInitial(LinkedList::new);

    /** */
    private final int poolSize;

    /** */
    private final Supplier<E> objFactory;

    /** */
    private final Consumer<E> closer;

    /** */
    private final Consumer<E> recycler;

    /**
     * @param poolSize number of objects which pool can contain.
     * @param objFactory factory used for new objects creation.
     * @param closer close callback.
     * @param recycler recycle callback.
     */
    public ThreadLocalObjectPool(int poolSize, Supplier<E> objFactory, Consumer<E> closer, Consumer<E> recycler) {
        this.poolSize = poolSize;
        this.objFactory = objFactory;
        this.closer = closer;
        this.recycler = recycler;
    }

    /**
     * Picks an object from the pool if one is present or creates new one otherwise.
     * Returns an object wrapper which could be returned to the pool.
     *
     * @return reusable object wrapper.
     */
    public Reusable borrow() {
        E obj = bag.get().poll();

        if (obj == null)
            obj = objFactory.get();

        return new Reusable(obj);
    }

    /**
     * Visible for test.
     *
     * @return pool size.
     */
    int bagSize() {
        return bag.get().size();
    }

    /**
     * Wrapper for a pooled object with capability to return the object to a pool.
     */
    public class Reusable {
        /** */
        private E object;

        /**
         * @param object Object to detach.
         */
        private Reusable(E object) {
            this.object = object;
        }

        /**
         * @return enclosed object.
         */
        public E object() {
            return object;
        }

        /**
         * Returns an object to a pool or closes it if the pool is already full.
         */
        public void recycle() {
            assert object != null : "The object is already recycled";

            if (bag.get().size() < poolSize) {
                if (recycler != null)
                    recycler.accept(object);

                bag.get().add(object);
            }
            else {
                if (closer != null)
                    closer.accept(object);
                else
                    U.closeQuiet(object);
            }

            object = null;
        }
    }
}
