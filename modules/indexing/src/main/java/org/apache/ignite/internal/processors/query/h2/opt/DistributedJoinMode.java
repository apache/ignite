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

package org.apache.ignite.internal.processors.query.h2.opt;

/**
 * Defines set of distributed join modes.
 */
public enum DistributedJoinMode {
    /**
     * Distributed joins is disabled. Local joins will be performed instead.
     */
    OFF,

    /**
     * Distributed joins is enabled within local node only.
     *
     * NOTE: This mode is used with segmented indices for local sql queries.
     * As in this case we need to make distributed join across local index segments
     * and prevent range-queries to other nodes.
     */
    LOCAL_ONLY,

    /**
     * Distributed joins is enabled.
     */
    ON;

    /**
     * @param isLocal Query local flag.
     * @param distributedJoins Query distributed joins flag.
     * @return DistributedJoinMode for the query.
     */
    public static DistributedJoinMode distributedJoinMode(boolean isLocal, boolean distributedJoins) {
        return distributedJoins ? (isLocal ? LOCAL_ONLY : ON) : OFF;
    }
}
