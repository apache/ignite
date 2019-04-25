/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;

/**
 *
 */
public interface CacheGetFuture {
    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearGetResponse res);
}
