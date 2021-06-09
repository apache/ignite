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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter;

class EntryMetaImpl implements RaftOutter.EntryMeta, RaftOutter.EntryMeta.Builder {
    private long term;
    private EnumOutter.EntryType type;
    private List<String> peersList = new ArrayList<>();
    private long dataLen;
    private List<String> oldPeersList = new ArrayList<>();
    private long checksum;
    private List<String> learnersList = new ArrayList<>();
    private List<String> oldLearnersList = new ArrayList<>();

    @Override public long getTerm() {
        return term;
    }

    @Override public EnumOutter.EntryType getType() {
        return type;
    }

    @Override public List<String> getPeersList() {
        return peersList;
    }

    @Override public int getPeersCount() {
        return peersList.size();
    }

    @Override public String getPeers(int index) {
        return peersList.get(index);
    }

    @Override public long getDataLen() {
        return dataLen;
    }

    @Override public List<String> getOldPeersList() {
        return oldPeersList;
    }

    @Override public int getOldPeersCount() {
        return oldPeersList.size();
    }

    @Override public String getOldPeers(int index) {
        return oldPeersList.get(index);
    }

    @Override public long getChecksum() {
        return checksum;
    }

    @Override public List<String> getLearnersList() {
        return learnersList;
    }

    @Override public int getLearnersCount() {
        return learnersList.size();
    }

    @Override public String getLearners(int index) {
        return learnersList.get(index);
    }

    @Override public List<String> getOldLearnersList() {
        return oldLearnersList;
    }

    @Override public int getOldLearnersCount() {
        return oldLearnersList.size();
    }

    @Override public String getOldLearners(int index) {
        return oldLearnersList.get(index);
    }

    @Override public RaftOutter.EntryMeta build() {
        return this;
    }

    @Override public Builder setTerm(long term) {
        this.term = term;

        return this;
    }

    @Override public Builder setChecksum(long checksum) {
        this.checksum = checksum;

        return this;
    }

    @Override public Builder setType(EnumOutter.EntryType type) {
        this.type = type;

        return this;
    }

    @Override public Builder setDataLen(int remaining) {
        this.dataLen = remaining;

        return this;
    }

    @Override public Builder addPeers(String peerId) {
        peersList.add(peerId);

        return this;
    }

    @Override public Builder addOldPeers(String oldPeerId) {
        oldPeersList.add(oldPeerId);

        return this;
    }

    @Override public Builder addLearners(String learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public Builder addOldLearners(String oldLearnerId) {
        oldLearnersList.add(oldLearnerId);

        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        EntryMetaImpl entryMeta = (EntryMetaImpl) o;

        if (term != entryMeta.term)
            return false;
        if (dataLen != entryMeta.dataLen)
            return false;
        if (checksum != entryMeta.checksum)
            return false;
        if (type != entryMeta.type)
            return false;
        if (!peersList.equals(entryMeta.peersList))
            return false;
        if (!oldPeersList.equals(entryMeta.oldPeersList))
            return false;
        if (!learnersList.equals(entryMeta.learnersList))
            return false;
        return oldLearnersList.equals(entryMeta.oldLearnersList);
    }

    @Override public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + type.hashCode();
        result = 31 * result + peersList.hashCode();
        result = 31 * result + (int) (dataLen ^ (dataLen >>> 32));
        result = 31 * result + oldPeersList.hashCode();
        result = 31 * result + (int) (checksum ^ (checksum >>> 32));
        result = 31 * result + learnersList.hashCode();
        result = 31 * result + oldLearnersList.hashCode();
        return result;
    }
}
