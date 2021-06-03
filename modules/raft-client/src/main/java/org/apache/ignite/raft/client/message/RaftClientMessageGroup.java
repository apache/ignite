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

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message types for the raft-client module.
 */
@MessageGroup(groupType = 2, groupName = "RaftClientMessages")
public class RaftClientMessageGroup {
    /**
     * Message type for {@link ActionRequest}.
     */
    public static final short ACTION_REQUEST = 0;

    /**
     * Message type for {@link ActionResponse}.
     */
    public static final short ACTION_RESPONSE = 1;

    /**
     * Message type for {@link AddLearnersRequest}.
     */
    public static final short ADD_LEARNERS_REQUEST = 2;

    /**
     * Message type for {@link AddPeersRequest}.
     */
    public static final short ADD_PEERS_REQUEST = 3;

    /**
     * Message type for {@link ChangePeersResponse}.
     */
    public static final short CHANGE_PEERS_RESPONSE = 4;

    /**
     * Message type for {@link GetLeaderRequest}.
     */
    public static final short GET_LEADER_REQUEST = 5;

    /**
     * Message type for {@link GetLeaderResponse}.
     */
    public static final short GET_LEADER_RESPONSE = 6;

    /**
     * Message type for {@link GetPeersRequest}.
     */
    public static final short GET_PEERS_REQUEST = 7;

    /**
     * Message type for {@link GetPeersResponse}.
     */
    public static final short GET_PEERS_RESPONSE = 8;

    /**
     * Message type for {@link RaftErrorResponse}.
     */
    public static final short RAFT_ERROR_RESPONSE = 9;

    /**
     * Message type for {@link RemoveLearnersRequest}.
     */
    public static final short REMOVE_LEARNERS_REQUEST = 10;

    /**
     * Message type for {@link RemovePeersRequest}.
     */
    public static final short REMOVE_PEERS_REQUEST = 11;

    /**
     * Message type for {@link SnapshotRequest}.
     */
    public static final short SNAPSHOT_REQUEST = 12;

    /**
     * Message type for {@link TransferLeadershipRequest}.
     */
    public static final short TRANSFER_LEADERSHIP_REQUEST = 13;
}
