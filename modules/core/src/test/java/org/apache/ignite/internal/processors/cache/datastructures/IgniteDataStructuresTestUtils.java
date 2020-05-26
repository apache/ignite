/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

public class IgniteDataStructuresTestUtils {
    /** */
    private IgniteDataStructuresTestUtils() {
        // No-op
    }

    /**
     * @return All variations of configurations according to backups and cache mode.
     */
    public static Map<String, AtomicConfiguration> getAtomicConfigurations() {
        Map<String, AtomicConfiguration> configs = new HashMap<>();

        for (int backups : Arrays.asList(0, 1)) {
            for (CacheMode cacheMode : CacheMode.values()) {
                if (cacheMode == CacheMode.LOCAL) {
                    // FIXME: https://issues.apache.org/jira/browse/IGNITE-13071
                    continue;
                }

                AtomicConfiguration cfg = new AtomicConfiguration()
                    .setBackups(backups)
                    .setCacheMode(cacheMode)
                    .setGroupName(cacheMode + "-grp-" + backups);

                AtomicConfiguration old = configs.put("" + backups + cacheMode, cfg);

                assert old == null : old;
            }
        }

        return configs;
    }

    /**
     * @return All variations of configurations according to atomicity mode, backups, cache mode and collocation flag.
     */
    public static Map<String, CollectionConfiguration> getCollectionConfigurations() {
        Map<String, CollectionConfiguration> configs = new HashMap<>();

        for (boolean collocated : Arrays.asList(true, false)) {
            for (CacheAtomicityMode atomicityMode : Arrays.asList(TRANSACTIONAL, ATOMIC)) {
                for (int backups : Arrays.asList(0, 1)) {
                    for (CacheMode cacheMode : CacheMode.values()) {
                        if (cacheMode == CacheMode.LOCAL) {
                            // FIXME: https://issues.apache.org/jira/browse/IGNITE-13071
                            continue;
                        }

                        CollectionConfiguration cfg = new CollectionConfiguration()
                            .setCollocated(collocated)
                            .setAtomicityMode(atomicityMode)
                            .setBackups(backups)
                            .setCacheMode(cacheMode)
                            .setGroupName(cacheMode + "-grp-" + backups);

                        CollectionConfiguration old = configs.put("" + collocated + atomicityMode + backups + cacheMode, cfg);

                        assert old == null : old;
                    }
                }
            }
        }

        return configs;
    }
}
