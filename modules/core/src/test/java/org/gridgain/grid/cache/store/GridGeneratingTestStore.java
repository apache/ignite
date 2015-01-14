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

package org.gridgain.grid.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test store that generates objects on demand.
 */
public class GridGeneratingTestStore implements GridCacheStore<String, String> {
    /** Number of entries to be generated. */
    private static final int DFLT_GEN_CNT = 100;

    /** */
    @IgniteCacheNameResource
    private String cacheName;

    /** {@inheritDoc} */
    @Override public String load(@Nullable IgniteTx tx, String key)
        throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<String, String> clo,
        @Nullable Object... args) throws IgniteCheckedException {
        if (args.length > 0) {
            try {
                int cnt = ((Number)args[0]).intValue();
                int postfix = ((Number)args[1]).intValue();

                for (int i = 0; i < cnt; i++)
                    clo.apply("key" + i, "val." + cacheName + "." + postfix);
            }
            catch (Exception e) {
                X.println("Unexpected exception in loadAll: " + e);

                throw new IgniteCheckedException(e);
            }
        }
        else {
            for (int i = 0; i < DFLT_GEN_CNT; i++)
                clo.apply("key" + i, "val." + cacheName + "." + i);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable IgniteTx tx,
        @Nullable Collection<? extends String> keys, IgniteBiInClosure<String, String> c) throws IgniteCheckedException {
        for (String key : keys)
            c.apply(key, "val" + key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, String key, @Nullable String val)
        throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable IgniteTx tx,
        @Nullable Map<? extends String, ? extends String> map) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable IgniteTx tx, String key)
        throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable IgniteTx tx,
        @Nullable Collection<? extends String> keys) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        // No-op.
    }
}
