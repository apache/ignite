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

package org.apache.ignite.source.flink;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Remote filter.
 */
public class TaskRemoteFilter implements IgnitePredicate<CacheEvent> {
    /** Serial version Id. */
    private static final long serialVersionUID = 1L;

    /** Ignite Instance Resource. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Cache name. */
    private final String cacheName;

    /** User-defined filter. */
    private final IgnitePredicate<CacheEvent> filter;

    /**
     * @param cacheName Cache name.
     * @param filter IgnitePredicate.
     */
    TaskRemoteFilter(String cacheName, IgnitePredicate<CacheEvent> filter) {
        this.cacheName = cacheName;
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(CacheEvent evt) {
        Affinity<Object> affinity = ignite.affinity(cacheName);

        // Process this event. Ignored on backups.
        return affinity.isPrimary(ignite.cluster().localNode(), evt.key()) &&
                (filter == null || filter.apply(evt));
    }
}
