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
package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.RpcRequests;

public class RequestVoteResponseImpl implements RpcRequests.RequestVoteResponse, RpcRequests.RequestVoteResponse.Builder {
    private long term;
    private boolean granted;

    @Override public long getTerm() {
        return term;
    }

    @Override public boolean getGranted() {
        return granted;
    }

    @Override public RpcRequests.RequestVoteResponse build() {
        return this;
    }

    @Override public Builder setTerm(long currTerm) {
        this.term = currTerm;

        return this;
    }

    @Override public Builder setGranted(boolean granted) {
        this.granted = granted;

        return this;
    }
}
