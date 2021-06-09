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

import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.util.Endpoint;

/**
 *
 */
public interface RpcClient extends Lifecycle<RpcOptions> {

    /**
     * Check connection for given address. // TODO asch rename to isAlive.
     *
     * @param endpoint target address
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Endpoint endpoint);

    /**
     * Register a connect event listener for the handler.
     *
     * @param handler The handler.
     */
    void registerConnectEventListener(final TopologyEventHandler handler);

    /**
     * Synchronous invocation.
     *
     * @param endpoint target address
     * @param request request object
     * @param timeoutMs timeout millisecond
     * @return invoke result
     */
    default Object invokeSync(final Endpoint endpoint, final Object request, final long timeoutMs)
        throws InterruptedException, RemotingException {
        return invokeSync(endpoint, request, null, timeoutMs);
    }

    /**
     * Synchronous invocation using a invoke context.
     *
     * @param endpoint target address
     * @param request request object
     * @param ctx invoke context
     * @param timeoutMs timeout millisecond
     * @return invoke result
     */
    Object invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
        final long timeoutMs) throws InterruptedException, RemotingException;

    /**
     * Asynchronous invocation with a callback.
     *
     * @param endpoint target address
     * @param request request object
     * @param callback invoke callback
     * @param timeoutMs timeout millisecond
     */
    default void invokeAsync(final Endpoint endpoint, final Object request, final InvokeCallback callback,
        final long timeoutMs) throws InterruptedException, RemotingException {
        invokeAsync(endpoint, request, null, callback, timeoutMs);
    }

    /**
     * Asynchronous invocation with a callback.
     *
     * @param endpoint target address
     * @param request request object
     * @param ctx invoke context
     * @param callback invoke callback
     * @param timeoutMs timeout millisecond
     */
    void invokeAsync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
        final InvokeCallback callback,
        final long timeoutMs) throws InterruptedException, RemotingException;
}
