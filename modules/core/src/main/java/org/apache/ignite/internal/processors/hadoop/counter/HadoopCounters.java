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

package org.apache.ignite.internal.processors.hadoop.counter;

import java.util.Collection;

/**
 * Counters store.
 */
public interface HadoopCounters {
    /**
     * Returns counter for the specified group and counter name. Creates new if it does not exist.
     *
     * @param grp Counter group name.
     * @param name Counter name.
     * @param cls Class for new instance creation if it's needed.
     * @return The counter that was found or added or {@code null} if create is false.
     */
    <T extends HadoopCounter> T counter(String grp, String name, Class<T> cls);

    /**
     * Returns all existing counters.
     *
     * @return Collection of counters.
     */
    Collection<HadoopCounter> all();

    /**
     * Merges all counters from another store with existing counters.
     *
     * @param other Counters to merge with.
     */
    void merge(HadoopCounters other);
}