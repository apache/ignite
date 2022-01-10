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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter.EntryMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.impl.SMThrowable;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.jetbrains.annotations.Nullable;

public final class RpcRequests {
    private RpcRequests() {
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.PING_REQUEST)
    public interface PingRequest extends Message {
        /**
         * <code>required int64 send_timestamp = 1;</code>
         */
        long sendTimestamp();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.ERROR_RESPONSE)
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

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.SM_ERROR_RESPONSE)
    public interface SMErrorResponse extends Message {
        /**
         * @return Throwable from client's state machine logic.
         */
        @Marshallable
        SMThrowable error();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.INSTALL_SNAPSHOT_REQUEST)
    public interface InstallSnapshotRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();

        @Marshallable
        RaftOutter.SnapshotMeta meta();

        String uri();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.INSTALL_SNAPSHOT_RESPONSE)
    public interface InstallSnapshotResponse extends Message {
        long term();

        boolean success();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.TIMEOUT_NOW_REQUEST)
    public interface TimeoutNowRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.TIMEOUT_NOW_RESPONSE)
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

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.REQUEST_VOTE_REQUEST)
    public interface RequestVoteRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();

        long lastLogTerm();

        long lastLogIndex();

        boolean preVote();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.REQUEST_VOTE_RESPONSE)
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

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.APPEND_ENTRIES_REQUEST)
    public interface AppendEntriesRequest extends Message {
        String groupId();

        String serverId();

        String peerId();

        long term();

        long prevLogTerm();

        long prevLogIndex();

        Collection<EntryMeta> entriesList();

        long committedIndex();

        @Marshallable
        ByteString data();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.APPEND_ENTRIES_RESPONSE)
    public interface AppendEntriesResponse extends Message {
        long term();

        boolean success();

        long lastLogIndex();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.GET_FILE_REQUEST)
    public interface GetFileRequest extends Message {
        long readerId();

        String filename();

        long count();

        long offset();

        boolean readPartly();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.GET_FILE_RESPONSE)
    public interface GetFileResponse extends Message {
        boolean eof();

        long readSize();

        @Marshallable
        ByteString data();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.READ_INDEX_REQUEST)
    public interface ReadIndexRequest extends Message {
        String groupId();

        String serverId();

        @Marshallable
        List<ByteString> entriesList();

        String peerId();
    }

    @Transferable(value = RaftMessageGroup.RpcRequestsMessageGroup.READ_INDEX_RESPONSE)
    public interface ReadIndexResponse extends Message {
        long index();

        boolean success();
    }
}
