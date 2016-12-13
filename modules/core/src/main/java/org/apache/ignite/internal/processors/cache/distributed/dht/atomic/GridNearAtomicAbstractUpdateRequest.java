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
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearAtomicAbstractUpdateRequest extends GridCacheMessage implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /**
     * @return Mapped node ID.
     */
    public abstract UUID nodeId();

    /**
     * @param nodeId Node ID.
     */
    public abstract void nodeId(UUID nodeId);

    /**
     * @return Subject ID.
     */
    public abstract UUID subjectId();

    /**
     * @return Task name hash.
     */
    public abstract int taskNameHash();

    /**
     * @return Future version.
     */
    public abstract GridCacheVersion futureVersion();

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    public abstract boolean fastMap();

    /**
     * @return Update version for fast-map request.
     */
    public abstract GridCacheVersion updateVersion();

    /**
     * @return Topology locked flag.
     */
    public abstract boolean topologyLocked();

    /**
     * @return {@code True} if request sent from client node.
     */
    public abstract boolean clientRequest();

    /**
     * @return Cache write synchronization mode.
     */
    public abstract CacheWriteSynchronizationMode writeSynchronizationMode();

    /**
     * @return Expiry policy.
     */
    public abstract ExpiryPolicy expiry();

    /**
     * @return Return value flag.
     */
    public abstract boolean returnValue();

    /**
     * @return Filter.
     */
    @Nullable public abstract CacheEntryPredicate[] filter();

    /**
     * @return Skip write-through to a persistent storage.
     */
    public abstract boolean skipStore();

    /**
     * @return Keep binary flag.
     */
    public abstract boolean keepBinary();

    /**
     * @return Update operation.
     */
    public abstract GridCacheOperation operation();

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public abstract Object[] invokeArguments();

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    public abstract boolean hasPrimary();

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    public abstract boolean onResponse(GridNearAtomicUpdateResponse res);

    /**
     * @return Response.
     */
    @Nullable public abstract GridNearAtomicUpdateResponse response();

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public abstract void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary);

    /**
     * @return Keys for this update request.
     */
    public abstract List<KeyCacheObject> keys();

    /**
     * @return Values for this update request.
     */
    public abstract List<?> values();

    /**
     * @param idx Key index.
     * @return Value.
     */
    public abstract CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    public abstract EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public abstract CacheObject writeValue(int idx);


    /**
     * @return Conflict versions.
     */
    @Nullable public abstract List<GridCacheVersion> conflictVersions();

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public abstract GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return Conflict TTL.
     */
    public abstract long conflictTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public abstract long conflictExpireTime(int idx);

    /**
     * Cleanup values.
     *
     * @param clearKeys If {@code true} clears keys.
     */
    public abstract void cleanup(boolean clearKeys);

    /**
     * @return Keys size.
     */
    public abstract int size();

    /**
     * @param idx Key index.
     * @return Key.
     */
    public abstract KeyCacheObject key(int idx);
}
