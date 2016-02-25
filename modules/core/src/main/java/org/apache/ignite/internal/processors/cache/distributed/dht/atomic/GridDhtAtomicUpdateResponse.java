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
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import java.util.Collection;

/**
 * Base interface for DHT atomic update responses.
 */
public interface GridDhtAtomicUpdateResponse extends Message {

    /**
     * Gets message lookup index. See {@link GridCacheMessage#lookupIndex()}.
     *
     * @return Message lookup index.
     */
    int lookupIndex();

    /**
     * @return Version assigned on primary node.
     */
    GridCacheVersion futureVersion();

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    void onError(IgniteCheckedException err);

    /**
     * @return Error, if any.
     */
    IgniteCheckedException error();

    /**
     * @return Failed keys.
     */
    Collection<KeyCacheObject> failedKeys();

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    void addFailedKey(KeyCacheObject key, Throwable e);

    /**
     * @return Evicted readers.
     */
    Collection<KeyCacheObject> nearEvicted();

    /**
     * Adds near evicted key..
     *
     * @param key Evicted key.
     */
    void addNearEvicted(KeyCacheObject key);

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
     * @return Message ID.
     */
    long messageId();
}
