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

package org.apache.ignite.raft.jraft;

import org.apache.ignite.network.annotations.MessageGroup;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;

/**
 * Message group for the Raft module.
 */
@MessageGroup(groupType = 3, groupName = "RaftMessages")
public class RaftMessageGroup {
    /**
     * Logical message subgroup for messages declared in {@link CliRequests}.
     */
    public static final class RpcClientMessageGroup {
        /** */
        public static final short ADD_PEER_REQUEST = 1000;

        /** */
        public static final short ADD_PEER_RESPONSE = 1001;

        /** */
        public static final short REMOVE_PEER_REQUEST = 1002;

        /** */
        public static final short REMOVE_PEER_RESPONSE = 1003;

        /** */
        public static final short CHANGE_PEERS_REQUEST = 1004;

        /** */
        public static final short CHANGE_PEERS_RESPONSE = 1005;

        /** */
        public static final short SNAPSHOT_REQUEST = 1006;

        /** */
        public static final short RESET_PEER_REQUEST = 1007;

        /** */
        public static final short TRANSFER_LEADER_REQUEST = 1008;

        /** */
        public static final short GET_LEADER_REQUEST = 1009;

        /** */
        public static final short GET_LEADER_RESPONSE = 1010;

        /** */
        public static final short GET_PEERS_REQUEST = 1011;

        /** */
        public static final short GET_PEERS_RESPONSE = 1012;

        /** */
        public static final short ADD_LEARNERS_REQUEST = 1013;

        /** */
        public static final short REMOVE_LEARNERS_REQUEST = 1014;

        /** */
        public static final short RESET_LEARNERS_REQUEST = 1015;

        /** */
        public static final short LEARNERS_OP_RESPONSE = 1016;
    }

    /**
     * Logical message subgroup for messages declared in {@link RaftOutter}.
     */
    public static final class RaftOutterMessageGroup {
        /** */
        public static final short ENTRY_META = 2000;

        /** */
        public static final short SNAPSHOT_META = 2001;

        /** */
        public static final short LOCAL_FILE_META = 2002;

        /** */
        public static final short STABLE_PB_META = 2003;

        /** */
        public static final short LOCAL_SNAPSHOT_PB_META = 2004;

        /** */
        public static final short LOCAL_SNAPSHOT_META_FILE = 2005;
    }

    /**
     * Logical message subgroup for messages declared in {@link RpcRequests}.
     */
    public static final class RpcRequestsMessageGroup {
        /** */
        public static final short PING_REQUEST = 3000;

        /** */
        public static final short ERROR_RESPONSE = 3001;

        /** */
        public static final short INSTALL_SNAPSHOT_REQUEST = 3002;

        /** */
        public static final short INSTALL_SNAPSHOT_RESPONSE = 3003;

        /** */
        public static final short TIMEOUT_NOW_REQUEST = 3004;

        /** */
        public static final short TIMEOUT_NOW_RESPONSE = 3005;

        /** */
        public static final short REQUEST_VOTE_REQUEST = 3006;

        /** */
        public static final short REQUEST_VOTE_RESPONSE = 3007;

        /** */
        public static final short APPEND_ENTRIES_REQUEST = 3008;

        /** */
        public static final short APPEND_ENTRIES_RESPONSE = 3009;

        /** */
        public static final short GET_FILE_REQUEST = 3010;

        /** */
        public static final short GET_FILE_RESPONSE = 3011;

        /** */
        public static final short READ_INDEX_REQUEST = 3012;

        /** */
        public static final short READ_INDEX_RESPONSE = 3013;

        /** */
        public static final short SM_ERROR_RESPONSE = 3014;
    }

    /**
     * Message types for Ignite actions.
     */
    public static final class RpcActionMessageGroup {
        /**
         * Message type for {@link ActionRequest}.
         */
        public static final short ACTION_REQUEST = 4000;

        /**
         * Message type for {@link ActionResponse}.
         */
        public static final short ACTION_RESPONSE = 4001;
    }
}
