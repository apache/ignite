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
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import java.nio.ByteBuffer;
import java.util.Collection;

public interface GridDhtAtomicUpdateResponse {
    int lookupIndex();

    GridCacheVersion futureVersion();

    void onError(IgniteCheckedException err);

    IgniteCheckedException error();

    Collection<KeyCacheObject> failedKeys();

    void addFailedKey(KeyCacheObject key, Throwable e);

    Collection<KeyCacheObject> nearEvicted();

    void addNearEvicted(KeyCacheObject key);

    void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException;

    void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    boolean addDeploymentInfo();

    boolean writeTo(ByteBuffer buf, MessageWriter writer);

    boolean readFrom(ByteBuffer buf, MessageReader reader);

    byte directType();

    byte fieldsCount();

    long messageId();
}
