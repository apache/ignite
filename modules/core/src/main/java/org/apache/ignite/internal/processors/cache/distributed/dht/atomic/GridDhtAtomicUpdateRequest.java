/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import javax.cache.processor.EntryProcessor;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

public interface GridDhtAtomicUpdateRequest {

    boolean forceTransformBackups();

    void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        int partId,
        @Nullable CacheObject prevVal,
        @Nullable Long updateIdx);

    void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime);

    int lookupIndex();

    UUID nodeId();

    UUID subjectId();

    int taskNameHash();

    int size();

    int nearSize();

    GridCacheVersion futureVersion();

    GridCacheVersion writeVersion();

    CacheWriteSynchronizationMode writeSynchronizationMode();

    AffinityTopologyVersion topologyVersion();

    Collection<KeyCacheObject> keys();

    KeyCacheObject key(int idx);

    int partitionId(int idx);

    Long updateCounter(int updCntr);

    KeyCacheObject nearKey(int idx);

    boolean keepBinary();

    @Nullable CacheObject value(int idx);

    @Nullable CacheObject previousValue(int idx);

    @Nullable CacheObject localPreviousValue(int idx);

    @Nullable EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    @Nullable CacheObject nearValue(int idx);

    @Nullable EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx);

    @Nullable GridCacheVersion conflictVersion(int idx);

    long ttl(int idx);

    long nearTtl(int idx);

    long conflictExpireTime(int idx);

    long nearExpireTime(int idx);

    boolean onResponse();

    @Nullable Object[] invokeArguments();

    void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException;

    void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    boolean addDeploymentInfo();

    boolean writeTo(ByteBuffer buf, MessageWriter writer);

    boolean readFrom(ByteBuffer buf, MessageReader reader);

    byte directType();

    byte fieldsCount();

    IgniteCheckedException classError();
}
