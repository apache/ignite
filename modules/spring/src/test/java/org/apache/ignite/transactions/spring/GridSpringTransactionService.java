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

package org.apache.ignite.transactions.spring;

import org.apache.ignite.IgniteCache;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service.
 */
public class GridSpringTransactionService {
    /**
     * @param cache Cache.
     * @param entryCnt Entries count.
     */
    @Transactional
    public void put(IgniteCache<Integer, String> cache, int entryCnt) {
        for (int i = 0; i < entryCnt; i++)
            cache.put(i, String.valueOf(i));
    }

    /**
     * @param cache Cache.
     * @param entryCnt Entries count.
     */
    @Transactional
    public void putWithError(IgniteCache<Integer, String> cache, int entryCnt) {
        for (int i = 0; i < entryCnt; i++)
            cache.put(i, String.valueOf(i));

        cache.put(Integer.valueOf("one"), "one");
    }

    /**
     * @param cache Cache.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void putWithMandatoryPropagation(IgniteCache<Integer, String> cache) {
        cache.put(1, "1");
    }

    /**
     * @param cache Cache.
     */
    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public void putWithUnsupportedIsolationLevel(IgniteCache<Integer, String> cache) {
        cache.put(1, "1");
    }
}
