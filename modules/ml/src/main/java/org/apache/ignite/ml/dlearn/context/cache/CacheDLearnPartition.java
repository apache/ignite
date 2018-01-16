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

package org.apache.ignite.ml.dlearn.context.cache;

import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;

/** */
public class CacheDLearnPartition<K, V> {
    /** */
    private static final String UPSTREAM_CACHE_NAME_KEY = "upstream_cache_name";

    /** */
    private static final String PART_KEY = "part";

    /** */
    private final DLearnPartitionStorage storage;

    /** */
    public CacheDLearnPartition(DLearnPartitionStorage storage) {
        this.storage = storage;
    }

    /** */
    public void setUpstreamCacheName(String upstreamCacheName) {
        storage.put(UPSTREAM_CACHE_NAME_KEY, upstreamCacheName);
    }

    /** */
    public String getUpstreamCacheName() {
        return storage.get(UPSTREAM_CACHE_NAME_KEY);
    }

    /** */
    public void setPart(int part) {
        storage.put(PART_KEY, part);
    }

    /** */
    public int getPart() {
        return storage.get(PART_KEY);
    }
}
