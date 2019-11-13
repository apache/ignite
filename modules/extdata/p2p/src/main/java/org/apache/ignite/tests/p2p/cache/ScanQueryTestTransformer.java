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

package org.apache.ignite.tests.p2p.cache;

import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Test class to verify p2p class loading for transformer of Scan Query
 * (see {@link IgniteCache#query(Query, IgniteClosure)}).
 */
public class ScanQueryTestTransformer implements IgniteClosure<Cache.Entry<Integer, Integer>, Integer> {
    /** */
    private final int scaleFactor;

    /** */
    public ScanQueryTestTransformer(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    /** {@inheritDoc} */
    @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
        return entry.getValue() * scaleFactor;
    }
}
