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

package org.apache.ignite.examples.servicegrid;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.services.*;

/**
 * Simple service which loops infinitely and prints out a counter.
 */
public class SimpleMapServiceImpl<K, V> implements Service, SimpleMapService<K, V> {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private static final String CACHE_NAME = SimpleMapServiceImpl.class.getSimpleName();

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Underlying cache map. */
    private IgniteCache<K, V> map;

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map.clear();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        ignite.destroyCache(CACHE_NAME);

        System.out.println("Service was cancelled: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(CACHE_NAME);

        map = ignite.getOrCreateCache(cfg);

        System.out.println("Service was initialized: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Executing distributed service: " + ctx.name());
    }
}
