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

import org.apache.ignite.raft.jraft.Status;

/**
 * The leader change context, contains:
 * <ul>
 * <li>leaderId: the leader peer id.</li>
 * <li>term: the leader term.</li>
 * <li>Status: context status.</li>
 * </ul>
 */
public class LeaderChangeContext {

    private PeerId leaderId;
    private long term;
    private Status status;

    public LeaderChangeContext(PeerId leaderId, long term, Status status) {
        super();
        this.leaderId = leaderId;
        this.term = term;
        this.status = status;
    }

    public PeerId getLeaderId() {
        return this.leaderId;
    }

    public void setLeaderId(PeerId leaderId) {
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return this.term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.leaderId == null ? 0 : this.leaderId.hashCode());
        result = prime * result + (this.status == null ? 0 : this.status.hashCode());
        result = prime * result + (int) (this.term ^ this.term >>> 32);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LeaderChangeContext other = (LeaderChangeContext) obj;
        if (this.leaderId == null) {
            if (other.leaderId != null) {
                return false;
            }
        }
        else if (!this.leaderId.equals(other.leaderId)) {
            return false;
        }
        if (this.status == null) {
            if (other.status != null) {
                return false;
            }
        }
        else if (!this.status.equals(other.status)) {
            return false;
        }
        return this.term == other.term;
    }

    @Override
    public String toString() {
        return "LeaderChangeContext [leaderId=" + this.leaderId + ", term=" + this.term + ", status=" + this.status
            + "]";
    }

}
