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
import org.apache.ignite.raft.jraft.util.ByteString;

public class GetFileResponseImpl implements RpcRequests.GetFileResponse, RpcRequests.GetFileResponse.Builder {
    private boolean eof;
    private long readSize;
    private ByteString data;

    @Override public boolean getEof() {
        return eof;
    }

    @Override public long getReadSize() {
        return readSize;
    }

    @Override public ByteString getData() {
        return data;
    }

    @Override public RpcRequests.GetFileResponse build() {
        return this;
    }

    @Override public Builder setReadSize(int read) {
        this.readSize = read;

        return this;
    }

    @Override public Builder setEof(boolean eof) {
        this.eof = eof;

        return this;
    }

    @Override public Builder setData(ByteString data) {
        this.data = data;

        return this;
    }
}
