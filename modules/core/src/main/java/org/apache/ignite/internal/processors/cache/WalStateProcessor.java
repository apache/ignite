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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Write-ahead log state processor. Manages WAL enable and disable.
 */
public class WalStateProcessor extends GridCacheSharedManagerAdapter {
    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();
    }

    public void onProposeDiscovery(WalStateProposeMessage msg) {
        // TODO
    }

    public void onPropose(WalStateProposeMessage msg) {
        // TODO
    }

    public void onFinishDiscovery(WalStateFinishMessage msg) {
        // TODO
    }

    public void onFinish(WalStateFinishMessage msg) {
        // TODO
    }

    public void onAck(WalStateAckMessage msg) {
        // TODO
    }

    public void onNodeLeft(ClusterNode node) {
        // TODO
    }
}
