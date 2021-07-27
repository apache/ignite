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
package org.apache.ignite.raft.jraft.rpc.impl.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeContext;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.TimeoutNowRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.TimeoutNowResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.rpc.impl.AbstractClientService;
import org.apache.ignite.raft.jraft.util.Endpoint;

/**
 * Raft rpc service.
 */
public class DefaultRaftClientService extends AbstractClientService implements RaftClientService {
    /** Stripes map */
    private final ConcurrentMap<Endpoint, Executor> appendEntriesExecutorMap = new ConcurrentHashMap<>();

    // cached node options
    private NodeOptions nodeOptions;

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            this.nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(final Endpoint endpoint, final RequestVoteRequest request,
        final RpcResponseClosure<RequestVoteResponse> done) {
        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> requestVote(final Endpoint endpoint, final RequestVoteRequest request,
        final RpcResponseClosure<RequestVoteResponse> done) {
        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> appendEntries(final Endpoint endpoint, final AppendEntriesRequest request,
        final int timeoutMs, final RpcResponseClosure<AppendEntriesResponse> done) {

        // Assign an executor in round-robin fasion.
        final Executor executor = this.appendEntriesExecutorMap.computeIfAbsent(endpoint,
            k -> nodeOptions.getStripedExecutor().next());

        if (connect(endpoint)) { // Replicator should be started asynchronously by node joined event.
            return invokeWithDone(endpoint, request, done, timeoutMs, executor);
        }

        return failedFuture(executor, request, done, endpoint);
    }

    @Override
    public Future<Message> getFile(final Endpoint endpoint, final GetFileRequest request, final int timeoutMs,
        final RpcResponseClosure<GetFileResponse> done) {
        // open checksum
        final InvokeContext ctx = new InvokeContext();

        return invokeWithDone(endpoint, request, ctx, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(final Endpoint endpoint, final InstallSnapshotRequest request,
        final RpcResponseClosure<InstallSnapshotResponse> done) {

        // Check connection before installing the snapshot to avoid waiting for undelivered message.
        if (connect(endpoint)) {
            return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcInstallSnapshotTimeout());
        }

        return failedFuture(rpcExecutor, request, done, endpoint);
    }

    @Override
    public Future<Message> timeoutNow(final Endpoint endpoint, final TimeoutNowRequest request, final int timeoutMs,
        final RpcResponseClosure<TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(final Endpoint endpoint, final ReadIndexRequest request, final int timeoutMs,
        final RpcResponseClosure<ReadIndexResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    /**
     * @param executor The executor to run done closure.
     * @param request The request.
     * @param done The closure.
     * @param endpoint The endpoint.
     * @return The future.
     */
    private Future<Message> failedFuture(Executor executor, Message request, RpcResponseClosure<?> done, Endpoint endpoint) {
        // fail-fast when no connection
        final CompletableFuture<Message> future = new CompletableFuture<>();

        executor.execute(() -> {
            if (done != null) {
                try {
                    done.run(new Status(RaftError.EINTERNAL, "Check connection[%s] fail and try to create new one", endpoint));
                }
                catch (final Throwable t) {
                    LOG.error("Fail to run RpcResponseClosure, the request is {}.", t, request);
                }
            }

            future.completeExceptionally(new RemotingException("Check connection[" +
                endpoint.toString() + "] fail and try to create new one"));
        });

        return future;
    }
}
