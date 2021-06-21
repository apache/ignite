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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface RpcClient extends Lifecycle<RpcOptions> {
    /**
     * Check connection for given address.
     *
     * @param endpoint target address
     * @return true if there is a connection and the connection is active and writable.
     * @deprecated // TODO asch remove IGNITE-14832
     */
    boolean checkConnection(Endpoint endpoint);

    /**
     * Register a connect event listener for the handler.
     *
     * @param handler The handler.
     */
    void registerConnectEventListener(TopologyEventHandler handler);

    /**
     * Asynchronous invocation with a callback.
     *
     * @param endpoint target address
     * @param request request object
     * @param ctx invoke context
     * @param callback invoke callback
     * @param timeoutMs timeout millisecond
     *
     * @return The future.
     */
    CompletableFuture<Message> invokeAsync(
        Endpoint endpoint,
        Object request,
        @Nullable InvokeContext ctx,
        InvokeCallback callback,
        long timeoutMs
    ) throws InterruptedException, RemotingException;
}
