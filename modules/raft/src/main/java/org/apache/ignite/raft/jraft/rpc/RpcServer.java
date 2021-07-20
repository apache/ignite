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

import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.rpc.impl.ConnectionClosedEventListener;

/**
 *
 */
public interface RpcServer<T> extends Lifecycle<T> {
    /**
     * Register a conn closed event listener.
     *
     * @param listener the event listener.
     */
    void registerConnectionClosedEventListener(ConnectionClosedEventListener listener);

    /**
     * Register user processor.
     *
     * @param processor the user processor which has a interest
     */
    void registerProcessor(RpcProcessor<?> processor);

    /**
     * @return bound port
     */
    int boundPort();
}
