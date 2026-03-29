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

import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/** Salvage finish request, near tx approval for tx with enabled writeThrough. */
public class GridDhtTxFinishSalvagedWriteThroughRequest extends GridDistributedBaseMessage {
    /** Empty constructor. */
    public GridDhtTxFinishSalvagedWriteThroughRequest() {
        // No-op.
    }

    /**
     * Approval request for uncommited salvaged transactions.
     *
     * @param ver Global transaction identifier within cluster, assigned by transaction coordinator.
     */
    public GridDhtTxFinishSalvagedWriteThroughRequest(GridCacheVersion ver) {
        super(ver, 0, false);
    }

    /** */
    @Override public short directType() {
        return 119;
    }
}
