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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Simple tests for {@link GridNearAtomicFullUpdateRequest}.
 */
public class GridNearAtomicFullUpdateRequestTest extends MarshallingAbstractTest {

    public void testMarshall() throws IgniteCheckedException {
        GridNearAtomicFullUpdateRequest req = new GridNearAtomicFullUpdateRequest(
            555,
            UUID.randomUUID(),
            new GridCacheVersion(1, 2, 3, 4),
            false,
            new GridCacheVersion(1, 2, 3, 4),
            new AffinityTopologyVersion(2),
            false,
            CacheWriteSynchronizationMode.PRIMARY_SYNC,
            GridCacheOperation.UPDATE,
            false,
            null,
            null,
            null,
            null,
            555,
            false,
            false,
            true,
            false,
            3,
            2
        );

        req.addUpdateEntry(new KeyCacheObjectImpl(1, null, 1), new CacheObjectImpl(2, null), 0, 0, null, true);
        req.addUpdateEntry(new KeyCacheObjectImpl(2, null, 1), new CacheObjectImpl(2, null), 0, 0, null, true);
        req.addUpdateEntry(new KeyCacheObjectImpl(3, null, 2), new CacheObjectImpl(2, null), 0, 0, null, true);

        GridNearAtomicFullUpdateRequest req2 = marshalUnmarshal(req);

        assertEquals(2, req2.stripeMap().size());

    }
}