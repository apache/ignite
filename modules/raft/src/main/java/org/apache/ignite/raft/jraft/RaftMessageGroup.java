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
        public static final short ADD_PEER_REQUEST = 0;

        /** */
        public static final short ADD_PEER_RESPONSE = 1;

        /** */
        public static final short REMOVE_PEER_REQUEST = 2;

        /** */
        public static final short REMOVE_PEER_RESPONSE = 3;

        /** */
        public static final short CHANGE_PEERS_REQUEST = 4;

        /** */
        public static final short CHANGE_PEERS_RESPONSE = 5;

        /** */
        public static final short SNAPSHOT_REQUEST = 6;

        /** */
        public static final short RESET_PEER_REQUEST = 7;

        /** */
        public static final short TRANSFER_LEADER_REQUEST = 8;

        /** */
        public static final short GET_LEADER_REQUEST = 9;

        /** */
        public static final short GET_LEADER_RESPONSE = 10;

        /** */
        public static final short GET_PEERS_REQUEST = 11;

        /** */
        public static final short GET_PEERS_RESPONSE = 12;

        /** */
        public static final short ADD_LEARNERS_REQUEST = 13;

        /** */
        public static final short REMOVE_LEARNERS_REQUEST = 14;

        /** */
        public static final short RESET_LEARNERS_REQUEST = 15;

        /** */
        public static final short LEARNERS_OP_RESPONSE = 16;
    }

    /**
     * Logical message subgroup for messages declared in {@link RaftOutter}.
     */
    public static final class RaftOutterMessageGroup {
        /** */
        public static final short ENTRY_META = 17;

        /** */
        public static final short SNAPSHOT_META = 18;

        /** */
        public static final short LOCAL_FILE_META = 19;

        /** */
        public static final short STABLE_PB_META = 20;

        /** */
        public static final short LOCAL_SNAPSHOT_PB_META = 21;

        /** */
        public static final short LOCAL_SNAPSHOT_META_FILE = 22;
    }

    /**
     * Logical message subgroup for messages declared in {@link RpcRequests}.
     */
    public static final class RpcRequestsMessageGroup {
        /** */
        public static final short PING_REQUEST = 23;

        /** */
        public static final short ERROR_RESPONSE = 24;

        /** */
        public static final short INSTALL_SNAPSHOT_REQUEST = 25;

        /** */
        public static final short INSTALL_SNAPSHOT_RESPONSE = 26;

        /** */
        public static final short TIMEOUT_NOW_REQUEST = 27;

        /** */
        public static final short TIMEOUT_NOW_RESPONSE = 28;

        /** */
        public static final short REQUEST_VOTE_REQUEST = 29;

        /** */
        public static final short REQUEST_VOTE_RESPONSE = 30;

        /** */
        public static final short APPEND_ENTRIES_REQUEST = 31;

        /** */
        public static final short APPEND_ENTRIES_RESPONSE = 32;

        /** */
        public static final short GET_FILE_REQUEST = 33;

        /** */
        public static final short GET_FILE_RESPONSE = 34;

        /** */
        public static final short READ_INDEX_REQUEST = 35;

        /** */
        public static final short READ_INDEX_RESPONSE = 36;
    }
}
