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

import java.util.List;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.jetbrains.annotations.Nullable;

public final class RpcRequests {
    private RpcRequests() {
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.PING_REQUEST, autoSerializable = false)
    public interface PingRequest extends Message {
        /**
         * <code>required int64 send_timestamp = 1;</code>
         */
        long sendTimestamp();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.ERROR_RESPONSE, autoSerializable = false)
    public interface ErrorResponse extends Message {
        /**
         * Error code.
         *
         * Note: despite of the naming, 0 code is the success response,
         * see {@link RaftError#SUCCESS}.
         *
         * @return error code.
         */
        int errorCode();

        /**
         * Error message.
         *
         * @return String with error message.
         */
        String errorMsg();

        /**
         * New leader id if provided, null otherwise.
         *
         * @return String new leader id, null otherwise.
         */
        @Nullable String leaderId();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.INSTALL_SNAPSHOT_REQUEST, autoSerializable = false)
    public interface InstallSnapshotRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();

        RaftOutter.SnapshotMeta meta();

        String uri();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.INSTALL_SNAPSHOT_RESPONSE, autoSerializable = false)
    public interface InstallSnapshotResponse extends Message {
        long term();

        boolean success();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.TIMEOUT_NOW_REQUEST, autoSerializable = false)
    public interface TimeoutNowRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.TIMEOUT_NOW_RESPONSE, autoSerializable = false)
    public interface TimeoutNowResponse extends Message {
        /**
         * <code>required int64 term = 1;</code>
         */
        long term();

        /**
         * <code>required bool success = 2;</code>
         */
        boolean success();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.REQUEST_VOTE_REQUEST, autoSerializable = false)
    public interface RequestVoteRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();

        long lastLogTerm();

        long lastLogIndex();

        boolean preVote();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.REQUEST_VOTE_RESPONSE, autoSerializable = false)
    public interface RequestVoteResponse extends Message {
        /**
         * <code>required int64 term = 1;</code>
         */
        long term();

        /**
         * <code>required bool granted = 2;</code>
         */
        boolean granted();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.APPEND_ENTRIES_REQUEST, autoSerializable = false)
    public interface AppendEntriesRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();

        long prevLogTerm();

        long prevLogIndex();

        List<RaftOutter.EntryMeta> entriesList();

        long committedIndex();

        ByteString data();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.APPEND_ENTRIES_RESPONSE, autoSerializable = false)
    public interface AppendEntriesResponse extends Message {
        long term();

        boolean success();

        long lastLogIndex();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.GET_FILE_REQUEST, autoSerializable = false)
    public interface GetFileRequest extends Message {
        long readerId();

        String filename();

        long count();

        long offset();

        boolean readPartly();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.GET_FILE_RESPONSE, autoSerializable = false)
    public interface GetFileResponse extends Message {
        boolean eof();

        long readSize();

        ByteString data();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.READ_INDEX_REQUEST, autoSerializable = false)
    public interface ReadIndexRequest extends Message {
        String groupId();

        String serverId();

        List<ByteString> entriesList();

        String peerId();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.READ_INDEX_RESPONSE, autoSerializable = false)
    public interface ReadIndexResponse extends Message {
        long index();

        boolean success();
    }
}
