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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Thread-safe pool for managing limited number objects for further reuse.
 *
 * @param <E> Pooled objects type.
 */
public final class ObjectPool<E extends AutoCloseable> {
    /**
     * Wrapper for a pooled object with capability to return the object to a pool.
     *
     * @param <T> Enclosed object type.
     */
    public static class Reusable<T extends AutoCloseable> {
        /** Object pool to recycle. */
        private final ObjectPool<T> pool;

        /** Detached object. */
        private T object;

        /**
         * @param pool Object pool.
         * @param object Detached object.
         */
        private Reusable(ObjectPool<T> pool, T object) {
            this.pool = pool;
            this.object = object;
        }

        /**
         * @return Enclosed object.
         */
        public T object() {
            return object;
        }

        /**
         * Returns an object to a pool or closes it if the pool is already full.
         */
        public void recycle() {
            assert object != null  : "Already recycled";

            if (pool.bag.size() < pool.poolSize) {
                pool.bag.add(object);

                if (pool.recycleLsnr != null)
                    pool.recycleLsnr.accept(object);
            }
            else {
                if (pool.closer == null)
                    U.closeQuiet(object);
                else
                    pool.closer.accept(object);
            }

            object = null;
        }
    }

    /** */
    private final Supplier<E> objectFactory;

    /** */
    private final ConcurrentLinkedQueue<E> bag = new ConcurrentLinkedQueue<>();

    /** */
    private final int poolSize;

    /** The function to close object. */
    private final Consumer<E> closer;

    /** The listener is called when object is returned to the pool. */
    private final Consumer<E> recycleLsnr;

    /**
     * @param objectFactory Factory used for new objects creation.
     * @param poolSize Number of objects which pool can contain.
     * @param closer Function to close object.
     * @param recycleLsnr The listener is called when object is returned to the pool.
     */
    public ObjectPool(Supplier<E> objectFactory, int poolSize, Consumer<E> closer, Consumer<E> recycleLsnr) {
        this.objectFactory = objectFactory;
        this.poolSize = poolSize;
        this.closer = closer;
        this.recycleLsnr =recycleLsnr;
    }

    /**
     * Picks an object from the pool if one is present or creates new one otherwise.
     * Returns an object wrapper which could be returned to the pool.
     *
     * @return Reusable object wrapper.
     */
    public Reusable<E> borrow() {
        E pooled = bag.poll();

        return new Reusable<>(this, pooled != null ? pooled : objectFactory.get());
    }

    /**
     * Visible for test
     * @return Pool bag size.
     */
    int bagSize() {
        return bag.size();
    }
}
