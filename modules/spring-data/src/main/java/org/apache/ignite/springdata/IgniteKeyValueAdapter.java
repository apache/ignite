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

package org.apache.ignite.springdata;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.util.CloseableIterator;

/**
 * Ignite's Spring Data Key-Value adapter implementation.
 */
public class IgniteKeyValueAdapter extends AbstractKeyValueAdapter {
    /** */
    private final Ignite ignite;

    /**
     * Instantiates {@code IgniteKeyValueAdapter} spawning an Apache Ignite client node with default
     * configuration parameters. The node will use the multicast protocol to discover an existing Apache Ignite
     * cluster and will connect to it if any.
     */
    public IgniteKeyValueAdapter() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setClientMode(true);

        ignite = Ignition.start(cfg);
    }

    /**
     * Instantiates {@code IgniteKeyValueAdapter} with already running Apache Ignite node.
     *
     * @param ignite Apache Ignite node.
     */
    public IgniteKeyValueAdapter(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Instantiates {@code IgniteKeyValueAdapter} spawning an Apache Ignite node with the Spring XML configuration
     * under {@code springCfgPath}.
     *
     * @param springCfgPath A path to Spring XML configuration.
     */
    public IgniteKeyValueAdapter(String springCfgPath) {
        ignite = Ignition.start(springCfgPath);
    }

    /**
     * Instantiates {@code IgniteKeyValueAdapter} spawning an Apache Ignite node with provided
     * {@code IgniteConfiguration}.
     *
     * @param cfg Ignite configuration.
     */
    public IgniteKeyValueAdapter(IgniteConfiguration cfg) {
        ignite = Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override public Object put(Serializable id, Object item, Serializable keyspace) {
        return cache(keyspace).getAndPut(id, item);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Serializable id, Serializable keyspace) {
        return cache(keyspace).containsKey(id);
    }

    /** {@inheritDoc} */
    @Override public Object get(Serializable id, Serializable keyspace) {
        return cache(keyspace).get(id);
    }

    /** {@inheritDoc} */
    @Override public Object delete(Serializable id, Serializable keyspace) {
        return cache(keyspace).invoke(id, new RemoveEntryProcessor());
    }

    /** {@inheritDoc} */
    @Override public Iterable<?> getAllOf(Serializable keyspace) {
        return new Iterable<Object>() {
            @Override public Iterator<Object> iterator() {
                return new CloseableIterator<Object>() {
                    Iterator<Cache.Entry<Serializable, Object>> iter = cache(keyspace).iterator();

                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override public Object next() {
                        Cache.Entry<Serializable, Object> entry = iter.next();

                        assert entry != null;

                        return entry.getValue();
                    }

                    @Override public void close() {
                        // Ignite's internal iterator is referenced by a weak reference. Releasing it.
                        iter = null;
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public CloseableIterator<Map.Entry<Serializable, Object>> entries(Serializable keyspace) {
        return new CloseableIterator<Map.Entry<Serializable, Object>>() {
            Iterator<Cache.Entry<Serializable, Object>> iter = cache(keyspace).iterator();

            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            @Override public Map.Entry<Serializable, Object> next() {
                Cache.Entry<Serializable, Object> entry = iter.next();

                assert entry != null;

                return new AbstractMap.SimpleEntry<Serializable, Object>(entry.getKey(), entry.getValue());
            }

            @Override public void close() {
                // Ignite's internal iterator is referenced by a weak reference. Releasing it.
                iter = null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void deleteAllOf(Serializable keyspace) {
        cache(keyspace).removeAll();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        for (String name : ignite.cacheNames())
            ignite.cache(name).removeAll();
    }

    /** {@inheritDoc} */
    @Override public long count(Serializable keyspace) {
        return cache(keyspace).size();
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        ignite.close();
    }

    /**
     * Gets {@code IgniteCache} instance.
     *
     * @param keyspace A keyspace.
     * @return Ignite cache instance.
     */
    private IgniteCache<Serializable, Object> cache(Serializable keyspace) {
        if (!keyspace.getClass().equals(String.class))
            throw new IgniteException("Keyspace must be of String type");

        return ignite.cache((String)keyspace);
    }

    /**
     *
     */
    private static class RemoveEntryProcessor implements EntryProcessor<Serializable, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Serializable, Object> entry, Object... arguments)
            throws EntryProcessorException {

            Object val = entry.getValue();

            entry.remove();

            return val;
        }
    }
}
