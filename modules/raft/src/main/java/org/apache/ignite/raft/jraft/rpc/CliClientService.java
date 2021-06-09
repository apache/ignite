/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.util.Endpoint;

/**
 * Cli RPC client service.
 */
public interface CliClientService extends ClientService {

    /**
     * Adds a peer.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> addPeer(Endpoint endpoint, CliRequests.AddPeerRequest request,
        RpcResponseClosure<CliRequests.AddPeerResponse> done);

    /**
     * Removes a peer.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> removePeer(Endpoint endpoint, CliRequests.RemovePeerRequest request,
        RpcResponseClosure<CliRequests.RemovePeerResponse> done);

    /**
     * Reset a peer.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> resetPeer(Endpoint endpoint, CliRequests.ResetPeerRequest request,
        RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Do a snapshot.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> snapshot(Endpoint endpoint, CliRequests.SnapshotRequest request,
        RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Change peers.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> changePeers(Endpoint endpoint, CliRequests.ChangePeersRequest request,
        RpcResponseClosure<CliRequests.ChangePeersResponse> done);

    /**
     * Add learners
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> addLearners(Endpoint endpoint, CliRequests.AddLearnersRequest request,
        RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Remove learners
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> removeLearners(Endpoint endpoint, CliRequests.RemoveLearnersRequest request,
        RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Reset learners
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> resetLearners(Endpoint endpoint, CliRequests.ResetLearnersRequest request,
        RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Get the group leader.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> getLeader(Endpoint endpoint, CliRequests.GetLeaderRequest request,
        RpcResponseClosure<CliRequests.GetLeaderResponse> done);

    /**
     * Transfer leadership to other peer.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> transferLeader(Endpoint endpoint, CliRequests.TransferLeaderRequest request,
        RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Get all peers of the replication group.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @return a future with result
     */
    Future<Message> getPeers(Endpoint endpoint, CliRequests.GetPeersRequest request,
        RpcResponseClosure<CliRequests.GetPeersResponse> done);
}
