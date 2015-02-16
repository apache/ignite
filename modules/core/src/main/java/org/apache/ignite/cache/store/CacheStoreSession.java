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

package org.apache.ignite.cache.store;

import org.apache.ignite.transactions.*;

import java.util.*;

/**
 * Session for the cache store operations.
 */
public interface CacheStoreSession {
    /**
     * Gets transaction spanning multiple store operations, or {@code null} if
     * there is no transaction.
     *
     * @return Transaction belonging to current session.
     */
    public Transaction transaction();

    /**
     * Gets current session properties. You can add properties directly to the
     * returned map.
     *
     * @return Current session properties.
     */
    public <K, V> Map<K, V> properties();

    /**
     * Cache name for the current store operation. Note that if the same store
     * is reused between different caches, then the cache name will change between
     * different store operations.
     *
     * @return Cache name for the current store operation.
     */
    public String cacheName();
}
