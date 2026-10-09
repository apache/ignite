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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/** Salvage tx. */
public class GridDhtTxSalvageMessage extends GridCacheMessage {
    /** */
    @Order(0)
    GridCacheVersion ver;

    /** Empty constructor. */
    public GridDhtTxSalvageMessage() {
        // No-op.
    }

    /**
     * @param ver Global transaction identifier within cluster, assigned by transaction coordinator.
     */
    public GridDhtTxSalvageMessage(GridCacheVersion ver) {
        this.ver = ver;
    }

    /** Tx version. */
    public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }
}
