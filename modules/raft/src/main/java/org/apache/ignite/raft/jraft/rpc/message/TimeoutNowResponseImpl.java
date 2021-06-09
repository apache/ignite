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

class TimeoutNowResponseImpl implements RpcRequests.TimeoutNowResponse, RpcRequests.TimeoutNowResponse.Builder {
    private long term;
    private boolean success;

    @Override public long getTerm() {
        return term;
    }

    @Override public boolean getSuccess() {
        return success;
    }

    @Override public RpcRequests.TimeoutNowResponse build() {
        return this;
    }

    @Override public Builder setTerm(long currTerm) {
        this.term = currTerm;

        return this;
    }

    @Override public Builder setSuccess(boolean success) {
        this.success = success;

        return this;
    }
}
