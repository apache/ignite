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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 *
 */
class IgniteDataStructuresTestUtils {
    /** */
    private IgniteDataStructuresTestUtils() {
        // No-op
    }

    /**
     * Performs given {@code action} for all presented elements of given {@code collection} and heck that
     * {@link IgniteClusterReadOnlyException} had been thrown.
     *
     * @param log Logger
     * @param collection Collection for applying action
     * @param nameExtractor Function for extraction element collection nam
     * @param action Action.
     * @param <E> Type of element's collection.
     */
    static <E> void performAction(
        IgniteLogger log,
        Collection<E> collection,
        Function<E, String> nameExtractor,
        Consumer<? super E> action
    ) {
        for (E e : collection) {
            Throwable ex = assertThrows(log, () -> action.accept(e), Exception.class, null);

            ClusterReadOnlyModeTestUtils.checkRootCause(ex, nameExtractor.apply(e));
        }
    }

    /**
     * @return All variations of configurations according to backups and cache mode.
     */
    public static Map<String, AtomicConfiguration> getAtomicConfigurations() {
        Map<String, AtomicConfiguration> configs = new HashMap<>();

        for (int backups : Arrays.asList(0, 1)) {
            for (CacheMode cacheMode : CacheMode.values()) {
                if (cacheMode == CacheMode.LOCAL) {
                    // TODO: https://issues.apache.org/jira/browse/IGNITE-13076
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
                            // TODO: https://issues.apache.org/jira/browse/IGNITE-13076
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
