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
import org.apache.ignite.lang.IgniteClosure;

/** */
public class ScanQueryTestTransformerWrapper {
    /** */
    private final int scaleFactor;

    /** */
    private final IgniteClosure<Cache.Entry<Integer, Integer>, Integer> clo = new IgniteClosure<Cache.Entry<Integer, Integer>, Integer>() {
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            return entry.getValue() * scaleFactor;
        }
    };

    /**
     * @param scaleFactor Scale factor.
     */
    public ScanQueryTestTransformerWrapper(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }
}
