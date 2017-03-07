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
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Trivial GridNearAtomicFullUpdateRequest tests.
 */
public class GridNearAtomicFullUpdateRequestTest extends MarshallingAbstractTest {

    /**
     * Message marshalling test.
     *
     * @throws IgniteCheckedException If fails.
     */
    public void testMarshall() throws IgniteCheckedException {
        GridCacheVersion updVer = new GridCacheVersion(1, 2, 3, 5);

        int entryNum = 3;

        GridNearAtomicFullUpdateRequest msg = new GridNearAtomicFullUpdateRequest(555,
            UUID.randomUUID(),
            555L,
            false,
            updVer,
            new AffinityTopologyVersion(25, 5),
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
            entryNum);

        for (int i = 0; i < entryNum; i++)
            msg.addUpdateEntry(
                key(i, i),
                val(i),
                -1,
                -1,
                null,
                true
            );

        GridNearAtomicFullUpdateRequest received = marshalUnmarshal(msg);

        assertEquals(Long.valueOf(555), received.futureVersion());
    }
}