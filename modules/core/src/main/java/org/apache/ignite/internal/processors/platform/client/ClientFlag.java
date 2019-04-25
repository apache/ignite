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

package org.apache.ignite.internal.processors.platform.client;

/**
 * Client response flag.
 */
public class ClientFlag {
    /**
     * No-op constructor to prevent instantiation.
     */
    private ClientFlag () {
        // No-op.
    }

    /**
     * @return Flags for response message.
     * @param error Error flag.
     * @param topologyChanged Affinity topology changed flag.
     */
    public static short makeFlags(boolean error, boolean topologyChanged) {
        short flags = 0;

        if (error)
            flags |= ClientFlag.ERROR;

        if (topologyChanged)
            flags |= ClientFlag.AFFINITY_TOPOLOGY_CHANGED;

        return flags;
    }

    /** Error flag. */
    public static final short ERROR = 1;

    /** Affinity topology change flag. */
    public static final short AFFINITY_TOPOLOGY_CHANGED = 1 << 1;
}
