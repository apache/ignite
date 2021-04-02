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

package org.apache.ignite.raft.client.message;

/**
 * A factory for immutable replication group messages.
 */
public interface RaftClientMessageFactory {
    /**
     * @return The builder.
     */
    AddPeersRequest.Builder addPeersRequest();

    /**
     * @return The builder.
     */
    RemovePeersRequest.Builder removePeerRequest();

    /**
     * @return The builder.
     */
    SnapshotRequest.Builder snapshotRequest();

    /**
     * @return The builder.
     */
    TransferLeadershipRequest.Builder transferLeaderRequest();

    /**
     * @return The builder.
     */
    GetLeaderRequest.Builder getLeaderRequest();

    /**
     * @return The builder.
     */
    GetLeaderResponse.Builder getLeaderResponse();

    /**
     * @return The builder.
     */
    GetPeersRequest.Builder getPeersRequest();

    /**
     * @return The builder.
     */
    GetPeersResponse.Builder getPeersResponse();

    /**
     * @return The builder.
     */
    AddLearnersRequest.Builder addLearnersRequest();

    /**
     * @return The builder.
     */
    RemoveLearnersRequest.Builder removeLearnersRequest();

    /**
     * @return The builder.
     */
    ChangePeersResponse.Builder changePeersResponse();

    /**
     * @return The builder.
     */
    ActionRequest.Builder actionRequest();

    /**
     * @return The builder.
     */
    ActionResponse.Builder actionResponse();

    /**
     * @return The builder.
     */
    RaftErrorResponse.Builder raftErrorResponse();
}
