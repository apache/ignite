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

/**
 * A raft node identifier.
 */
public final class NodeId implements Serializable {
    private static final long serialVersionUID = 4428173460056804264L;

    /**
     * Raft group id
     */
    private final String groupId;

    /**
     * Node peer id
     */
    private final PeerId peerId;

    /**
     * cached toString result
     */
    private String str;

    public NodeId(String groupId, PeerId peerId) {
        super();
        this.groupId = groupId;
        this.peerId = peerId;
    }

    public String getGroupId() {
        return this.groupId;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = "<" + this.groupId + "/" + this.peerId + ">";
        }
        return str;
    }

    public PeerId getPeerId() {
        return this.peerId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.groupId == null ? 0 : this.groupId.hashCode());
        result = prime * result + (this.peerId == null ? 0 : this.peerId.hashCode());
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
        final NodeId other = (NodeId) obj;
        if (this.groupId == null) {
            if (other.groupId != null) {
                return false;
            }
        }
        else if (!this.groupId.equals(other.groupId)) {
            return false;
        }
        if (this.peerId == null) {
            return other.peerId == null;
        }
        else {
            return this.peerId.equals(other.peerId);
        }
    }
}
