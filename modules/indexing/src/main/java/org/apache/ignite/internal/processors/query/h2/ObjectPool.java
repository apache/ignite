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

import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.*;
import java.util.function.Supplier;

/**
 * Not thread-safe pool for managing limited number objects for further reuse.
 *
 * @param <E> pooled objects type
 */
public final class ObjectPool<E extends AutoCloseable> {
    /**
     * Wrapper for a pooled object with capability to return the object to a pool.
     *
     * @param <T> enclosed object type
     */
    public static class Reusable<T extends AutoCloseable> {
        private final ObjectPool<T> pool;
        private final T object;

        private Reusable(ObjectPool<T> pool, T object) {
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
            if (pool.bag.size() < pool.poolSize)
                pool.bag.add(this);
            else
                U.closeQuiet(object);
        }
    }

    private final Supplier<E> objectFactory;
    private final Queue<Reusable<E>> bag;
    private final int poolSize;

    /**
     * @param objectFactory factory used for new objects creation
     * @param poolSize number of objects which pool can contain
     */
    public ObjectPool(Supplier<E> objectFactory, int poolSize) {
        this.objectFactory = objectFactory;
        this.bag = new LinkedList<>();
        this.poolSize = poolSize;
    }

    /**
     * Picks an object from the pool if one is present or creates new one otherwise.
     * Returns an object wrapper which could be returned to the pool.
     *
     * @return reusable object wrapper
     */
    public Reusable<E> borrow() {
        Reusable<E> pooled = bag.poll();
        return pooled != null ? pooled : new Reusable<>(this, objectFactory.get());
    }
}
