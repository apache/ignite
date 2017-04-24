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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Index closure accepting current entry state.
 */
public interface SchemaIndexCacheVisitorClosure {
    /**
     * Apply closure.
     *
     * @param key Key.
     * @param part Partition.
     * @param val Value.
     * @param ver Version.
     * @param expiration Expiration.
     * @param link Link.
     * @throws IgniteCheckedException If failed.
     */
    public void apply(KeyCacheObject key, int part, CacheObject val, GridCacheVersion ver, long expiration, long link)
        throws IgniteCheckedException;
}
