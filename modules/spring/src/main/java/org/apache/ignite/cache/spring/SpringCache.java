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

package org.apache.ignite.cache.spring;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.springframework.cache.*;
import org.springframework.cache.support.*;

import java.io.*;

/**
 * Spring cache implementation.
 */
class SpringCache implements Cache, Serializable {
    /** */
    private String name;

    /** */
    private Ignite ignite;

    /** */
    private CacheProjection<Object, Object> cache;

    /** */
    private IgniteClosure<Object, Object> keyFactory;

    /**
     * @param name Cache name.
     * @param ignite Ignite instance.
     * @param cache Cache.
     * @param keyFactory Key factory.
     */
    SpringCache(String name,
        Ignite ignite,
        CacheProjection<?, ?> cache,
        IgniteClosure<Object, Object> keyFactory)
    {
        assert cache != null;

        this.name = name;
        this.ignite = ignite;
        this.cache = (CacheProjection<Object, Object>)cache;
        this.keyFactory = keyFactory != null ? keyFactory : F.identity();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Object getNativeCache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public Cache.ValueWrapper get(Object key) {
        try {
            Object val = cache.get(keyFactory.apply(key));

            return val != null ? new SimpleValueWrapper(val) : null;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to get value from cache [cacheName=" + cache.name() +
                ", key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T get(Object key, Class<T> type) {
        try {
            Object val = cache.get(keyFactory.apply(key));

            if (val != null && type != null && !type.isInstance(val))
                throw new IllegalStateException("Cached value is not of required type [cacheName=" + cache.name() +
                    ", key=" + key + ", val=" + val + ", requiredType=" + type + ']');

            return (T)val;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to get value from cache [cacheName=" + cache.name() +
                ", key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) {
        try {
            cache.putx(keyFactory.apply(key), val);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to put value to cache [cacheName=" + cache.name() +
                ", key=" + key + ", val=" + val + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public ValueWrapper putIfAbsent(Object key, Object val) {
        try {
            Object old = cache.putIfAbsent(keyFactory.apply(key), val);

            return old != null ? new SimpleValueWrapper(old) : null;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to put value to cache [cacheName=" + cache.name() +
                ", key=" + key + ", val=" + val + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) {
        try {
            cache.removex(keyFactory.apply(key));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to remove value from cache [cacheName=" + cache.name() +
                ", key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            ignite.compute(cache.gridProjection()).broadcast(new ClearClosure(cache));
        }
        catch (IgniteException e) {
            throw new IgniteException("Failed to clear cache [cacheName=" + cache.name() + ']', e);
        }
    }

    /**
     * Closure that removes all entries from cache.
     */
    private static class ClearClosure extends CAX implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache projection. */
        private CacheProjection<Object, Object> cache;

        /**
         * For {@link Externalizable}.
         */
        public ClearClosure() {
            // No-op.
        }

        /**
         * @param cache Cache projection.
         */
        private ClearClosure(CacheProjection<Object, Object> cache) {
            this.cache = cache;
        }

        /** {@inheritDoc} */
        @Override public void applyx() throws IgniteCheckedException {
            cache.removeAll();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(cache);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cache = (CacheProjection<Object, Object>)in.readObject();
        }
    }
}
