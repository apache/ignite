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

import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface GridNearAtomicUpdateRequest {
    /**
     * @return Mapped node ID.
     */
    public UUID nodeId();

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId);

    /**
     * @return Subject ID.
     */
    public UUID subjectId();

    /**
     * @return Task name hash.
     */
    public int taskNameHash();

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion();

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    public boolean fastMap();

    /**
     * @return Update version for fast-map request.
     */
    public GridCacheVersion updateVersion();

    /**
     * @return Topology locked flag.
     */
    public boolean topologyLocked();

    /**
     * @return {@code True} if request sent from client node.
     */
    public boolean clientRequest();

    /**
     * @return Cache write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode();

    /**
     * @return Expiry policy.
     */
    public ExpiryPolicy expiry();

    /**
     * @return Return value flag.
     */
    public boolean returnValue();

    /**
     * @return Filter.
     */
    @Nullable public CacheEntryPredicate[] filter();

    /**
     * @return Skip write-through to a persistent storage.
     */
    public boolean skipStore();

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary();

    /**
     * @return Update operation.
     */
    public GridCacheOperation operation();

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public Object[] invokeArguments();

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    public boolean hasPrimary();

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    public boolean onResponse(GridNearAtomicUpdateResponse res);

    /**
     * @return Response.
     */
    @Nullable public GridNearAtomicUpdateResponse response();

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary);

    /**
     * @return Keys for this update request.
     */
    public List<KeyCacheObject> keys();

    /**
     * @return Values for this update request.
     */
    public List<?> values();

    /**
     * @param idx Key index.
     * @return Value.
     */
    public CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    public EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public CacheObject writeValue(int idx);

    /**
     * @return Message ID.
     */
    public long messageId();

    /**
     * Gets topology version or -1 in case of topology version is not required for this message.
     *
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion();

    /**
     * @return Conflict versions.
     */
    @Nullable public List<GridCacheVersion> conflictVersions();

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return Conflict TTL.
     */
    public long conflictTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public long conflictExpireTime(int idx);

    /**
     * Cleanup values.
     *
     * @param clearKeys If {@code true} clears keys.
     */
    public void cleanup(boolean clearKeys);
}
