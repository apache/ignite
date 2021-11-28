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

package org.apache.ignite.internal.table.distributed;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;

/**
 * Transaction manager extension for Ignite tables.
 *
 * <p>Uses raft protocol to replicate tx finish state for a partition group.
 */
public class TableTxManagerImpl extends TxManagerImpl {
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private static final int FINISH_TIMEOUT = 1000;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param lockManager    Lock manager.
     */
    public TableTxManagerImpl(ClusterService clusterService, LockManager lockManager) {
        super(clusterService, lockManager);
    }

    /** {@inheritDoc} */
    @Override
    protected CompletableFuture<?> finish(String groupId, Timestamp ts, boolean commit) {
        ActionRequest req = FACTORY.actionRequest().command(new FinishTxCommand(ts, commit)).groupId(groupId).readOnlySafe(true).build();

        return clusterService.messagingService().invoke(clusterService.topologyService().localMember(), req, FINISH_TIMEOUT);
    }
}
