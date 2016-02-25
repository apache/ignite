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
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Base interface for near atomic update responses.
 */
public interface GridNearAtomicUpdateResponseInterface extends Message {

    /**
     * Gets message lookup index. See {@link GridCacheMessage#lookupIndex()}.
     *
     * @return Message lookup index.
     */
    int lookupIndex();

    /**
     * @return Mapped node ID.
     */
    UUID nodeId();

    /**
     * @param nodeId Node ID.
     */
    void nodeId(UUID nodeId);

    /**
     * @return Future version.
     */
    GridCacheVersion futureVersion();

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    void error(IgniteCheckedException err);

    /**
     * @return Error, if any.
     */
    IgniteCheckedException error();

    /**
     * @return Collection of failed keys.
     */
    Collection<KeyCacheObject> failedKeys();

    /**
     * @return Return value.
     */
    GridCacheReturn returnValue();

    /**
     * @param ret Return value.
     */
    @SuppressWarnings("unchecked") void returnValue(GridCacheReturn ret);

    /**
     * @param remapKeys Remap keys.
     */
    void remapKeys(List<KeyCacheObject> remapKeys);

    /**
     * @return Remap keys.
     */
    Collection<KeyCacheObject> remapKeys();

    /**
     * Adds value to be put in near cache on originating node.
     *
     * @param keyIdx Key index.
     * @param val Value.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    void addNearValue(int keyIdx,
        @Nullable CacheObject val,
        long ttl,
        long expireTime);

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach") void addNearTtl(int keyIdx, long ttl, long expireTime);

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    long nearExpireTime(int idx);

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    long nearTtl(int idx);

    /**
     * @param nearVer Version generated on primary node to be used for originating node's near cache update.
     */
    void nearVersion(GridCacheVersion nearVer);

    /**
     * @return Version generated on primary node to be used for originating node's near cache update.
     */
    GridCacheVersion nearVersion();

    /**
     * @param keyIdx Index of key for which update was skipped
     */
    void addSkippedIndex(int keyIdx);

    /**
     * @return Indexes of keys for which update was skipped
     */
    @Nullable List<Integer> skippedIndexes();

    /**
     * @return Indexes of keys for which values were generated on primary node.
     */
    @Nullable List<Integer> nearValuesIndexes();

    /**
     * @param idx Index.
     * @return Value generated on primary node which should be put to originating node's near cache.
     */
    @Nullable CacheObject nearValue(int idx);

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    void addFailedKey(KeyCacheObject key, Throwable e);

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     */
    void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e);

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     * @param ctx Context.
     */
    void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e, GridCacheContext ctx);

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
}
