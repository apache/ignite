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

import java.io.Serializable;
import org.apache.ignite.raft.jraft.util.Bits;
import org.apache.ignite.raft.jraft.util.Copiable;
import org.apache.ignite.raft.jraft.util.CrcUtil;

/**
 * Log identifier.
 */
public class LogId implements Comparable<LogId>, Copiable<LogId>, Serializable, Checksum {

    private static final long serialVersionUID = -6680425579347357313L;

    private long index;
    private long term;

    @Override
    public LogId copy() {
        return new LogId(this.index, this.term);
    }

    @Override
    public long checksum() {
        byte[] bs = new byte[16];
        Bits.putLong(bs, 0, this.index);
        Bits.putLong(bs, 8, this.term);
        return CrcUtil.crc64(bs);
    }

    public LogId() {
        this(0, 0);
    }

    public LogId(final long index, final long term) {
        super();
        setIndex(index);
        setTerm(term);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.index ^ (this.index >>> 32));
        result = prime * result + (int) (this.term ^ (this.term >>> 32));
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogId other = (LogId) obj;
        if (this.index != other.index) {
            return false;
        }
        // noinspection RedundantIfStatement
        if (this.term != other.term) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(final LogId o) {
        // Compare term at first
        final int c = Long.compare(getTerm(), o.getTerm());
        if (c == 0) {
            return Long.compare(getIndex(), o.getIndex());
        }
        else {
            return c;
        }
    }

    public long getTerm() {
        return this.term;
    }

    public void setTerm(final long term) {
        this.term = term;
    }

    public long getIndex() {
        return this.index;
    }

    public void setIndex(final long index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "LogId [index=" + this.index + ", term=" + this.term + "]";
    }

}
