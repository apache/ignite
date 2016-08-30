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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCacheV2 extends VisorCache {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Flag indicating that cache has near cache.
     */
    private boolean near;

    /** {@inheritDoc} */
    @Override public VisorCache from(IgniteEx ignite, String cacheName, int sample) throws IgniteCheckedException {
        VisorCache c = super.from(ignite, cacheName, sample);

        if (c != null && c instanceof VisorCacheV2) {
            GridCacheAdapter ca = ignite.context().cache().internalCache(cacheName);

            // Process only started caches.
            if (ca != null && ca.context().started())
                ((VisorCacheV2)c).near = ca.context().isNear();
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override protected VisorCache initHistory(VisorCache c) {
        super.initHistory(c);

        if (c instanceof VisorCacheV2)
            ((VisorCacheV2) c).near = near;

        return c;
    }

    /** {@inheritDoc} */
    @Override public VisorCache history() {
        return initHistory(new VisorCacheV2());
    }

    /**
     * @return {@code true} if cache has near cache.
     */
    public boolean near() {
        return near;
    }
}
