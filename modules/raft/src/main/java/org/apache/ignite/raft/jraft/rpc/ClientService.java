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

import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.util.Endpoint;

/**
 * RPC client service
 */
public interface ClientService extends Lifecycle<RpcOptions> {
    /**
     * Connect to endpoint, returns true when success. TODO asch rename to isAlive IGNITE-14832.
     *
     * @param endpoint server address
     * @return true on connect success
     */
    boolean connect(final Endpoint endpoint);

    /**
     * Send a requests and waits for response with callback, returns the request future.
     *
     * @param endpoint server address
     * @param request request data
     * @param done callback
     * @param timeoutMs timeout millis
     * @return a future with operation result
     */
    <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
        final RpcResponseClosure<T> done, final int timeoutMs);
}
