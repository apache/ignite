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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;

/**
 * Projection tests for replicated cache.
 */
public class GridCacheReplicatedProjectionSelfTest extends GridCacheAbstractProjectionSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testInvalidateFlag() throws Exception {
        try {
            for (int i = 1; i < 3; i++)
                startGrid(i);

            String key = "1";
            Integer val = Integer.valueOf(key);

            // Put value into cache.
            cache(0).put(key, val);

            for (int i = 0; i < 3; i++)
                assertEquals(val, grid(i).cache(null).peek(key));

            // Put value again, remote nodes are backups and should not be invalidated.
            cache(0).flagsOn(INVALIDATE).put(key, val);

            for (int i = 0; i < 3; i++) {
                Object peeked = grid(i).cache(null).peek(key);

                assertEquals(val, peeked);
            }
        }
        finally {
            for (int i = 1; i < 3; i++)
                stopGrid(i);
        }
    }
}
