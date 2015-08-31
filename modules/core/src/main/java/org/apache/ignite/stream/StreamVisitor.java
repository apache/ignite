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

package org.apache.ignite.stream;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteBiInClosure;

/**
 * Convenience adapter to visit every key-value tuple in the stream. Note, that the visitor
 * does not update the cache. If the tuple needs to be stored in the cache,
 * then {@code cache.put(...)} should be called explicitly.
 */
public abstract class StreamVisitor<K, V> implements StreamReceiver<K, V>, IgniteBiInClosure<IgniteCache<K, V>, Map.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException {
        for (Map.Entry<K, V> entry : entries)
            apply(cache, entry);
    }

    /**
     * Creates a new visitor based on instance of {@link IgniteBiInClosure}.
     *
     * @param c Closure.
     * @return Stream visitor.
     */
    public static <K, V> StreamVisitor<K, V> from(final IgniteBiInClosure<IgniteCache<K, V>, Map.Entry<K, V>> c) {
        return new StreamVisitor<K, V>() {
            @Override public void apply(IgniteCache<K, V> cache, Map.Entry<K, V> entry) {
                c.apply(cache, entry);
            }
        };
    }
}