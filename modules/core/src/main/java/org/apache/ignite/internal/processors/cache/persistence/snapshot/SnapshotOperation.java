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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Set;

/**
 * Initial snapshot operation interface.
 */
public interface SnapshotOperation extends Serializable {
    /**
     * Cache group ids included to this snapshot.
     *
     * @return Cache group identifiers.
     */
    public Set<Integer> cacheGroupIds();

    /**
     * @return Cache names included to this snapshot.
     */
    public Set<String> cacheNames();

    /**
     * @return Any custom extra parameter.
     * In case Map object is provided, contains named snapshot operation attributes.
     */
    public Object extraParameter();
}
