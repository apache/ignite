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

/**
 * Wrapper for a pooled object with capability to return the object to a pool.
 *
 * @param <T> Enclosed object type.
 */
public class ObjectPoolReusable<T extends AutoCloseable> {
    /** Object pool to recycle. */
    private final ObjectPool<T> pool;

    /** Detached object. */
    private T object;

    /**
     * @param pool Object pool.
     * @param object Detached object.
     */
    ObjectPoolReusable(ObjectPool<T> pool, T object) {
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

        pool.recycle(object);

        object = null;
    }
}
