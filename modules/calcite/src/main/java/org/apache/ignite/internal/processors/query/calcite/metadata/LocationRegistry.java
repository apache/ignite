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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 *
 */
public interface LocationRegistry {
    Location single(AffinityTopologyVersion topVer); // returns local node with single partition
    Location random(AffinityTopologyVersion topVer); // returns random distribution, partitions count depends on nodes count
    Location distributed(int cacheId, AffinityTopologyVersion topVer); // returns cache distribution
}
