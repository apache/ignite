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
package org.apache.ignite.raft.jraft.entity;

import java.nio.ByteBuffer;

/**
 * User log entry.
 */
public class UserLog {

    /** log index */
    private long index;
    /** log data */
    private ByteBuffer data;

    public UserLog(long index, ByteBuffer data) {
        super();
        this.index = index;
        this.data = data;
    }

    public long getIndex() {
        return this.index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    public void reset() {
        this.data.clear();
        this.index = 0;
    }

    @Override
    public String toString() {
        return "UserLog [index=" + this.index + ", data=" + this.data + "]";
    }
}
