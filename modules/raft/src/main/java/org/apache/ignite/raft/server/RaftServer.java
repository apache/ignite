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

package org.apache.ignite.raft.server;

import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;

/**
 * The RAFT protocol based replication server.
 * <p>
 * Supports multiple RAFT groups.
 * <p>
 * The server listens for client commands, submits them to a replicated log and calls {@link RaftGroupCommandListener}
 * {@code onRead} and {@code onWrite} methods then after the command was committed to the log.
 */
public interface RaftServer {
    /**
     * @return Local member.
     */
    NetworkMember localMember();

    /**
     * Set a listener for group commands.
     * @param groupId group id.
     * @param lsnr Listener.
     */
    void setListener(String groupId, RaftGroupCommandListener lsnr);

    /**
     * Remove a command listener.
     * @param groupId Group id.
     */
    void clearListener(String groupId);

    /**
     * Shutdown a server.
     *
     * @throws Exception
     */
    void shutdown() throws Exception;
}
