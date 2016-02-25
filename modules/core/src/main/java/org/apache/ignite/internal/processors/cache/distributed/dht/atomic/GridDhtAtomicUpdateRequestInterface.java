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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;
import javax.cache.processor.EntryProcessor;
import java.util.Collection;
import java.util.UUID;

/**
 * Base interface for DHT atomic update requests.
 */
public interface GridDhtAtomicUpdateRequestInterface extends Message {

    /**
     * @return Force transform backups flag.
     */
    boolean forceTransformBackups();

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} adds previous value.
     * @param prevVal Previous value.
     */
    void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        int partId,
        @Nullable CacheObject prevVal,
        @Nullable Long updateIdx,
        boolean storeLocPrevVal);

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime);

    /**
     * Gets message lookup index. See {@link GridCacheMessage#lookupIndex()}.
     *
     * @return Message lookup index.
     */
    int lookupIndex();

    /**
     * @return Node ID.
     */
    UUID nodeId();

    /**
     * @return Subject ID.
     */
    UUID subjectId();

    /**
     * @return Task name.
     */
    int taskNameHash();

    /**
     * @return Keys size.
     */
    int size();

    /**
     * @return Keys size.
     */
    int nearSize();

    /**
     * @return Version assigned on primary node.
     */
    GridCacheVersion futureVersion();

    /**
     * @return Write version.
     */
    GridCacheVersion writeVersion();

    /**
     * @return Cache write synchronization mode.
     */
    CacheWriteSynchronizationMode writeSynchronizationMode();

    /**
     * @return Topology version.
     */
    AffinityTopologyVersion topologyVersion();

    /**
     * @return Keys.
     */
    Collection<KeyCacheObject> keys();

    /**
     * @param idx Key index.
     * @return Key.
     */
    KeyCacheObject key(int idx);

    /**
     * @param idx Partition index.
     * @return Partition id.
     */
    int partitionId(int idx);

    /**
     * @param updCntr Update counter.
     * @return Update counter.
     */
    Long updateCounter(int updCntr);

    /**
     * @param idx Near key index.
     * @return Key.
     */
    KeyCacheObject nearKey(int idx);

    /**
     * @return Keep binary flag.
     */
    boolean keepBinary();

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable CacheObject previousValue(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable CacheObject localPreviousValue(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Nullable EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Nullable CacheObject nearValue(int idx);

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Nullable EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx);

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return TTL.
     */
    long ttl(int idx);

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    long nearTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    long conflictExpireTime(int idx);

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    long nearExpireTime(int idx);

    /**
     * @return {@code True} if on response flag changed.
     */
    boolean onResponse();

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable Object[] invokeArguments();

    /**
     * This method is called before the whole message is serialized
     * and is responsible for pre-marshalling state.
     *
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException;

    /**
     * This method is called after the message is deserialized and is responsible for
     * unmarshalling state marshalled in {@link #prepareMarshal(GridCacheSharedContext)} method.
     *
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    /**
     *  Deployment enabled flag indicates whether deployment info has to be added to this message.
     *
     * @return {@code true} or if deployment info must be added to the the message, {@code false} otherwise.
     */
    boolean addDeploymentInfo();

    /**
     * @return Error.
     */
    IgniteCheckedException classError();
}
