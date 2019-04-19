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

package org.apache.ignite.internal.processors.hadoop.state;

import org.apache.ignite.internal.processors.hadoop.HadoopSharedMap;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

import java.util.Collection;
import java.util.UUID;

/**
 * Shared state for HadoopGroupingTest.
 */
public class HadoopGroupingTestState {
    /** Values. */
    private static final GridConcurrentHashSet<UUID> vals = HadoopSharedMap.map(HadoopGroupingTestState.class)
        .put("vals", new GridConcurrentHashSet<UUID>());

    /**
     * @return Values.
     */
    public static Collection<UUID> values() {
        return vals;
    }
}
