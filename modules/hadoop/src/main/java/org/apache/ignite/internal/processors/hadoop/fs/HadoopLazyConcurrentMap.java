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

package org.apache.ignite.internal.processors.hadoop.fs;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jsr166.ConcurrentHashMap8;

/**
 * Maps values by keys.
 * Values are created lazily using {@link ValueFactory}.
 *
 * Despite of the name, does not depend on any Hadoop classes.
 */
public class HadoopLazyConcurrentMap<K, V extends Closeable> {
    /** The map storing the actual values. */
    private final ConcurrentMap<K, ValueWrapper> map = new ConcurrentHashMap8<>();

    /** The factory passed in by the client. Will be used for lazy value creation. */
    private final ValueFactory<K, V> factory;

    /** Lock used to close the objects. */
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();

    /** Flag indicating that this map is closed and cleared. */
    private boolean closed;

    /**
     * Constructor.
     * @param factory the factory to create new values lazily.
     */
    public HadoopLazyConcurrentMap(ValueFactory<K, V> factory) {
        this.factory = factory;

        assert getClass().getClassLoader() == Ignite.class.getClassLoader();
    }

    /**
     * Gets cached or creates a new value of V.
     * Never returns null.
     * @param k the key to associate the value with.
     * @return the cached or newly created value, never null.
     * @throws IgniteException on error
     */
    public V getOrCreate(K k) {
        ValueWrapper w = map.get(k);

        if (w == null) {
            closeLock.readLock().lock();

            try {
                if (closed)
                    throw new IllegalStateException("Failed to create value for key [" + k
                        + "]: the map is already closed.");

                final ValueWrapper wNew = new ValueWrapper(k);

                w = map.putIfAbsent(k, wNew);

                if (w == null) {
                    wNew.init();

                    w = wNew;
                }
            }
            finally {
                closeLock.readLock().unlock();
            }
        }

        try {
            V v = w.getValue();

            assert v != null;

            return v;
        }
        catch (IgniteCheckedException ie) {
            throw new IgniteException(ie);
        }
    }

    /**
     * Clears the map and closes all the values.
     */
    public void close() throws IgniteCheckedException {
        closeLock.writeLock().lock();

        try {
            if (closed)
                return;

            closed = true;

            Exception err = null;

            Set<K> keySet = map.keySet();

            for (K key : keySet) {
                V v = null;

                try {
                    v = map.get(key).getValue();
                }
                catch (IgniteCheckedException ignore) {
                    // No-op.
                }

                if (v != null) {
                    try {
                        v.close();
                    }
                    catch (Exception err0) {
                        if (err == null)
                            err = err0;
                    }
                }
            }

            map.clear();

            if (err != null)
                throw new IgniteCheckedException(err);
        }
        finally {
            closeLock.writeLock().unlock();
        }
    }

    /**
     * Helper class that drives the lazy value creation.
     */
    private class ValueWrapper {
        /** Future. */
        private final GridFutureAdapter<V> fut = new GridFutureAdapter<>();

        /** the key */
        private final K key;

        /**
         * Creates new wrapper.
         */
        private ValueWrapper(K key) {
            this.key = key;
        }

        /**
         * Initializes the value using the factory.
         */
        private void init() {
            try {
                final V v0 = factory.createValue(key);

                if (v0 == null)
                    throw new IgniteException("Failed to create non-null value. [key=" + key + ']');

                fut.onDone(v0);
            }
            catch (Throwable e) {
                fut.onDone(e);
            }
        }

        /**
         * Gets the available value or blocks until the value is initialized.
         * @return the value, never null.
         * @throws IgniteCheckedException on error.
         */
        V getValue() throws IgniteCheckedException {
            return fut.get();
        }
    }

    /**
     * Interface representing the factory that creates map values.
     * @param <K> the type of the key.
     * @param <V> the type of the value.
     */
    public interface ValueFactory <K, V> {
        /**
         * Creates the new value. Should never return null.
         *
         * @param key the key to create value for
         * @return the value.
         * @throws IgniteException on failure.
         */
        public V createValue(K key);
    }
}