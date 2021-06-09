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

public class GetFileRequestImpl implements RpcRequests.GetFileRequest, RpcRequests.GetFileRequest.Builder {
    private long readerId;
    private String filename;
    private long count;
    private long offset;
    private boolean readPartly;

    @Override public long getReaderId() {
        return readerId;
    }

    @Override public String getFilename() {
        return filename;
    }

    @Override public long getCount() {
        return count;
    }

    @Override public long getOffset() {
        return offset;
    }

    @Override public boolean getReadPartly() {
        return readPartly;
    }

    @Override public RpcRequests.GetFileRequest build() {
        return this;
    }

    @Override public Builder setCount(long cnt) {
        this.count = cnt;

        return this;
    }

    @Override public Builder setOffset(long offset) {
        this.offset = offset;

        return this;
    }

    @Override public Builder setReadPartly(boolean readPartly) {
        this.readPartly = readPartly;

        return this;
    }

    @Override public Builder setFilename(String fileName) {
        this.filename = fileName;

        return this;
    }

    @Override public Builder setReaderId(long readerId) {
        this.readerId = readerId;

        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GetFileRequestImpl that = (GetFileRequestImpl) o;

        if (readerId != that.readerId)
            return false;
        if (count != that.count)
            return false;
        if (offset != that.offset)
            return false;
        if (readPartly != that.readPartly)
            return false;
        return filename.equals(that.filename);
    }

    @Override public int hashCode() {
        int result = (int) (readerId ^ (readerId >>> 32));
        result = 31 * result + filename.hashCode();
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (readPartly ? 1 : 0);
        return result;
    }
}
