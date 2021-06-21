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
package org.apache.ignite.raft.jraft.rpc.impl;

import java.net.ConnectException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.InvokeTimeoutException;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.ClientService;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.InvokeContext;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcClient;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.apache.ignite.raft.jraft.util.internal.ThrowUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract RPC client service based.
 */
public abstract class AbstractClientService implements ClientService, TopologyEventHandler {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractClientService.class);

    protected volatile RpcClient rpcClient;
    protected ExecutorService rpcExecutor;
    protected RpcOptions rpcOptions;

    /**
     * The set of pinged addresses
     */
    protected Set<String> readyAddresses = new ConcurrentHashSet<>();

    public RpcClient getRpcClient() {
        return this.rpcClient;
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        if (this.rpcClient != null) {
            return true;
        }
        this.rpcOptions = rpcOptions;
        return initRpcClient(this.rpcOptions.getRpcProcessorThreadPoolSize());
    }

    @Override public void onAppeared(ClusterNode member) {
        // No-op. TODO asch https://issues.apache.org/jira/browse/IGNITE-14843
    }

    @Override public void onDisappeared(ClusterNode member) {
        readyAddresses.remove(member.address());
    }

    protected void configRpcClient(final RpcClient rpcClient) {
        rpcClient.registerConnectEventListener(this);
    }

    protected boolean initRpcClient(final int rpcProcessorThreadPoolSize) {
        this.rpcClient = rpcOptions.getRpcClient();

        configRpcClient(this.rpcClient);

        // TODO asch should the client be created lazily? A client doesn't make sence without a server IGNITE-14832
        this.rpcClient.init(null);

        this.rpcExecutor = ((RpcOptions)rpcOptions).getClientExecutor();

        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.rpcClient != null) {
            this.rpcClient.shutdown();
            this.rpcClient = null;
        }
    }

    @Override
    public boolean connect(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        if (rc == null) {
            throw new IllegalStateException("Client service is uninitialized.");
        }

        // Remote node is alive and pinged, safe to continue.
        if (readyAddresses.contains(endpoint.toString()))
            return true;

        // Remote node must be pinged to make sure listeners are set.
        synchronized (this) {
            if (readyAddresses.contains(endpoint.toString()))
                return true;

            try {
                final RpcRequests.PingRequest req = RpcRequests.PingRequest.newBuilder()
                    .setSendTimestamp(System.currentTimeMillis())
                    .build();

                Future<Message> fut =
                    invokeWithDone(endpoint, req, null, null, rpcOptions.getRpcConnectTimeoutMs(), rpcExecutor);

                final ErrorResponse resp = (ErrorResponse) fut.get(); // Future will be certainly terminated by timeout.

                if (resp != null && resp.getErrorCode() == 0) {
                    readyAddresses.add(endpoint.toString());

                    return true;
                }
                else
                    return false;
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (final ExecutionException e) {
                LOG.error("Fail to connect {}, exception: {}.", endpoint, e.getMessage());
            }
        }

        return false;
    }

    @Override
    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
        final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, done, timeoutMs, this.rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
        final RpcResponseClosure<T> done, final int timeoutMs,
        final Executor rpcExecutor) {
        return invokeWithDone(endpoint, request, null, done, timeoutMs, rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
        final InvokeContext ctx,
        final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs, this.rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
        final InvokeContext ctx,
        final RpcResponseClosure<T> done, final int timeoutMs,
        final Executor rpcExecutor) {
        final RpcClient rc = this.rpcClient;
        final FutureImpl<Message> future = new FutureImpl<>();
        final Executor currExecutor = rpcExecutor != null ? rpcExecutor : this.rpcExecutor;

        try {
            if (rc == null) {
                // TODO asch replace with ignite exception, check all places IGNITE-14832
                future.completeExceptionally(new IllegalStateException("Client service is uninitialized."));
                // should be in another thread to avoid dead locking.
                Utils.runClosureInExecutor(currExecutor, done, new Status(RaftError.EINTERNAL,
                    "Client service is uninitialized."));
                return future;
            }

            return rc.invokeAsync(endpoint, request, ctx, new InvokeCallback() {
                @Override
                public void complete(final Object result, final Throwable err) {
                    if (err == null) {
                        Status status = Status.OK();
                        Message msg;
                        if (result instanceof ErrorResponse) {
                            status = handleErrorResponse((ErrorResponse) result);
                            msg = (Message) result;
                        }
                        else {
                            msg = (Message) result;
                        }
                        if (done != null) {
                            try {
                                if (status.isOk()) {
                                    done.setResponse((T) msg);
                                }
                                done.run(status);
                            }
                            catch (final Throwable t) {
                                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                            }
                        }
                        if (!future.isDone()) {
                            future.complete(msg);
                        }
                    }
                    else {
                        if (ThrowUtil.hasCause(err, null, ConnectException.class))
                            readyAddresses.remove(endpoint.toString()); // Force logical reconnect.

                        if (done != null) {
                            try {
                                done.run(new Status(err instanceof InvokeTimeoutException ? RaftError.ETIMEDOUT
                                    : RaftError.EINTERNAL, "RPC exception:" + err.getMessage()));
                            }
                            catch (final Throwable t) {
                                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                            }
                        }
                        if (!future.isDone()) {
                            future.completeExceptionally(err);
                        }
                    }
                }

                @Override
                public Executor executor() {
                    return currExecutor;
                }
            }, timeoutMs <= 0 ? this.rpcOptions.getRpcDefaultTimeout() : timeoutMs);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
            // should be in another thread to avoid dead locking.
            Utils.runClosureInExecutor(currExecutor, done,
                new Status(RaftError.EINTR, "Sending rpc was interrupted"));
        }
        catch (final RemotingException e) {
            future.completeExceptionally(e);
            // should be in another thread to avoid dead locking.
            Utils.runClosureInExecutor(currExecutor, done, new Status(RaftError.EINTERNAL,
                "Fail to send a RPC request:" + e.getMessage()));
        }

        return future;
    }

    private static Status handleErrorResponse(final ErrorResponse eResp) {
        final Status status = new Status();
        status.setCode(eResp.getErrorCode());
        status.setErrorMsg(eResp.getErrorMsg());
        return status;
    }
}
