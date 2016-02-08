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
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface GridNearAtomicUpdateResponse {

    int lookupIndex();

    UUID nodeId();

    void nodeId(UUID nodeId);

    GridCacheVersion futureVersion();

    void error(IgniteCheckedException err);

    IgniteCheckedException error();

    Collection<KeyCacheObject> failedKeys();

    GridCacheReturn returnValue();

    @SuppressWarnings("unchecked") void returnValue(GridCacheReturn ret);

    void remapKeys(List<KeyCacheObject> remapKeys);

    Collection<KeyCacheObject> remapKeys();

    void addNearValue(int keyIdx,
        @Nullable CacheObject val,
        long ttl,
        long expireTime);

    @SuppressWarnings("ForLoopReplaceableByForEach") void addNearTtl(int keyIdx, long ttl, long expireTime);

    long nearExpireTime(int idx);

    long nearTtl(int idx);

    void nearVersion(GridCacheVersion nearVer);

    GridCacheVersion nearVersion();

    void addSkippedIndex(int keyIdx);

    @Nullable List<Integer> skippedIndexes();

    @Nullable List<Integer> nearValuesIndexes();

    @Nullable CacheObject nearValue(int idx);

    void addFailedKey(KeyCacheObject key, Throwable e);

    void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e);

    void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e, GridCacheContext ctx);

    void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException;

    void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    boolean addDeploymentInfo();

    boolean writeTo(ByteBuffer buf, MessageWriter writer);

    boolean readFrom(ByteBuffer buf, MessageReader reader);

    byte directType();

    byte fieldsCount();
}
