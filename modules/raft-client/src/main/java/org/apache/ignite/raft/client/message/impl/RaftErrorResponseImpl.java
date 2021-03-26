/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.message.impl;

import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.message.RaftErrorResponse;

public class RaftErrorResponseImpl implements RaftErrorResponse, RaftErrorResponse.Builder {
    /** */
    private RaftErrorCode errorCode;

    /** */
    private Peer newLeader;

    /** {@inheritDoc} */
    @Override public RaftErrorCode errorCode() {
        return errorCode;
    }

    /** {@inheritDoc} */
    @Override public Peer newLeader() {
        return newLeader;
    }

    /** {@inheritDoc} */
    @Override public Builder errorCode(RaftErrorCode errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder newLeader(Peer newLeader) {
        this.newLeader = newLeader;

        return this;
    }

    /** {@inheritDoc} */
    @Override public RaftErrorResponse build() {
        return this;
    }
}
