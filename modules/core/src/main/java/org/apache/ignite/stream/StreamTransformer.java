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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;

/**
 * Convenience adapter to transform update existing values in streaming cache
 * based on the previously cached value.
 */
public abstract class StreamTransformer<K, V> implements StreamReceiver<K, V>, EntryProcessor<K, V, Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException {
        for (Map.Entry<K, V> entry : entries)
            cache.invoke(entry.getKey(), this);
    }

    /**
     * Creates a new transformer based on instance of {@link CacheEntryProcessor}.
     *
     * @param ep Entry processor.
     * @return Stream transformer.
     */
    public static <K, V> StreamTransformer<K, V> from(final CacheEntryProcessor<K, V, Object> ep) {
        return new StreamTransformer<K, V>() {
            @Override public Object process(MutableEntry<K, V> entry, Object... args) throws EntryProcessorException {
                return ep.process(entry, args);
            }
        };
    }
}