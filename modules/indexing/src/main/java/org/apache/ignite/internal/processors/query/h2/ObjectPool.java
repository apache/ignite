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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Thread-safe pool for managing limited number objects for further reuse.
 *
 * @param <E> Pooled objects type.
 */
public final class ObjectPool<E extends AutoCloseable> {
    /** */
    private final Supplier<E> objectFactory;

    /** */
    private final ConcurrentLinkedQueue<E> bag = new ConcurrentLinkedQueue<>();

    /** */
    private final int poolSize;

    /** The function to close object. */
    private final Consumer<E> closer;

    /**
     * @param objectFactory Factory used for new objects creation.
     * @param poolSize Number of objects which pool can contain.
     * @param closer Function to close object.
     */
    public ObjectPool(Supplier<E> objectFactory, int poolSize, Consumer<E> closer) {
        this.objectFactory = objectFactory;
        this.poolSize = poolSize;
        this.closer = closer != null ? closer : U::closeQuiet;
    }

    /**
     * Picks an object from the pool if one is present or creates new one otherwise.
     * Returns an object wrapper which could be returned to the pool.
     *
     * @return Reusable object wrapper.
     */
    public ObjectPoolReusable<E> borrow() {
        E pooled = bag.poll();

        return new ObjectPoolReusable<>(this, pooled != null ? pooled : objectFactory.get());
    }

    /**
     * Recycles an object.
     *
     * @param object Object.
     */
    void recycle(E object) {
        assert object != null  : "Already recycled";

        if (bag.size() < poolSize)
            bag.add(object);
        else
            closer.accept(object);
    }

    /**
     * Visible for test
     * @return Pool bag size.
     */
    int bagSize() {
        return bag.size();
    }
}
