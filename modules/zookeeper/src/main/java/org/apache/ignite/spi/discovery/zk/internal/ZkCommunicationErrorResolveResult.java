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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkCommunicationErrorResolveResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    final GridLongList killedNodes;

    /** */
    final Exception err;

    /**
     * @param killedNodes Killed nodes.
     * @param err Error.
     */
    ZkCommunicationErrorResolveResult(@Nullable GridLongList killedNodes, Exception err) {
        this.killedNodes = killedNodes;
        this.err = err;
    }
}
