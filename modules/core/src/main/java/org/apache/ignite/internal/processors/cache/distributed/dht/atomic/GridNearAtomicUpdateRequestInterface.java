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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.List;
import java.util.UUID;

/**
 * Base interface for near atomic update interfaces.
 */
public interface GridNearAtomicUpdateRequestInterface {
    public List<KeyCacheObject> keys();

    public AffinityTopologyVersion topologyVersion();

    public GridCacheVersion futureVersion();

    public boolean returnValue();

    public int taskNameHash();

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    public boolean fastMap();

    /**
     * @return Update version for fast-map request.
     */
    public GridCacheVersion updateVersion();

    public boolean clientRequest();

    public boolean topologyLocked();

    public ExpiryPolicy expiry();

    public boolean skipStore();

    public GridCacheOperation operation();

    public CacheWriteSynchronizationMode writeSynchronizationMode();

    public UUID subjectId();

    @Nullable public Object[] invokeArguments();

    public boolean keepBinary();

    @Nullable public CacheEntryPredicate[] filter();

    public UUID nodeId();

    public void nodeId(UUID nodeId);

    public boolean hasPrimary();

    @Nullable public List<GridCacheVersion> conflictVersions();

    @Nullable public GridCacheVersion conflictVersion(int idx);

    public long conflictTtl(int idx);

    public long conflictExpireTime(int idx);

    public List<?> values();

    public CacheObject value(int idx);

    public long messageId();

    public EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    public CacheObject writeValue(int idx);
}
