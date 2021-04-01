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

package org.apache.ignite.raft.client.message;

import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.jetbrains.annotations.Nullable;

/**
 * Raft error response. Also used as a default response when errorCode == {@link RaftErrorCode#SUCCESS}
 */
public interface RaftErrorResponse extends NetworkMessage {
    /**
     * @return Error code.
     */
    public RaftErrorCode errorCode();

    /**
     * @return The new leader if a current leader is obsolete or null if not applicable.
     */
    public @Nullable Peer newLeader();

    /** */
    public interface Builder {
        /**
         * @param errorCode Error code.
         * @return The builder.
         */
        Builder errorCode(RaftErrorCode errorCode);

        /**
         * @param newLeader New leader.
         * @return The builder.
         */
        Builder newLeader(Peer newLeader);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        RaftErrorResponse build();
    }
}
