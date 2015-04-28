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
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import javax.cache.*;

/**
 *
 */
public class IgniteNearPrimaryNodeChangedRemoveSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setNearConfiguration(new NearCacheConfiguration());
        ccfg.setInterceptor(new Interceptor());

        return ccfg;
    }

    /** {@inheritDoc} */
    public void testRemove() throws Exception {
        for (int i = 0; i < 100; i++)
            grid(0).cache(null).put(i, i);

        // Start two more grids, some near entries will become invalid.
        // TODO IGNITE-424 Test passes if additional grids start is commented out.
        startGrid(3);
        startGrid(4);

        try {
            for (int i = 0; i < 100; i++)
                grid(2).cache(null).remove(i);

            info(">>>>>>>> Finished remove");

            for (int i = 0; i < 100; i++)
                grid(0).cache(null).put(i, -1);

            for (int i = 0; i < 100; i++)
                assertEquals(null, grid(0).cache(null).get(i));
        }
        finally {
            stopGrid(3);
            stopGrid(4);
        }
    }

    /**
     *
     */
    private static class Interceptor extends CacheInterceptorAdapter {
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            if (!F.eq(newVal, -1))
                return newVal;

            return entry.getValue() == null ? null : newVal;
        }
    }
}
