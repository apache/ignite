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
package org.apache.ignite.raft.jraft;

import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.ReadOnlyServiceOptions;

/**
 * The read-only query service.
 */
public interface ReadOnlyService extends Lifecycle<ReadOnlyServiceOptions> {

    /**
     * Adds a ReadIndex request.
     *
     * @param reqCtx request context of readIndex
     * @param closure callback
     */
    void addRequest(final byte[] reqCtx, final ReadIndexClosure closure);

    /**
     * Waits for service shutdown.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    void join() throws InterruptedException;

    /**
     * Called when the node is turned into error state.
     *
     * @param error error with raft info
     */
    void setError(final RaftException error);

}
