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
import org.apache.ignite.raft.jraft.entity.RaftOutter;

class SnapshotMetaImpl implements RaftOutter.SnapshotMeta, RaftOutter.SnapshotMeta.Builder {
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private List<String> peersList = new ArrayList<>();
    private List<String> oldPeersList = new ArrayList<>();
    private List<String> learnersList = new ArrayList<>();
    private List<String> oldLearnersList = new ArrayList<>();

    @Override public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override public long getLastIncludedTerm() {
        return lastIncludedTerm;
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

    @Override public List<String> getOldPeersList() {
        return oldPeersList;
    }

    @Override public int getOldPeersCount() {
        return oldPeersList.size();
    }

    @Override public String getOldPeers(int index) {
        return oldPeersList.get(index);
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

    @Override public RaftOutter.SnapshotMeta build() {
        return this;
    }

    @Override public Builder setLastIncludedIndex(long lastAppliedIndex) {
        this.lastIncludedIndex = lastAppliedIndex;

        return this;
    }

    @Override public Builder setLastIncludedTerm(long lastAppliedTerm) {
        this.lastIncludedTerm = lastAppliedTerm;

        return this;
    }

    @Override public Builder addPeers(String peerId) {
        peersList.add(peerId);

        return this;
    }

    @Override public Builder addLearners(String learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public Builder addOldPeers(String oldPeerId) {
        oldPeersList.add(oldPeerId);

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

        SnapshotMetaImpl that = (SnapshotMetaImpl) o;

        if (lastIncludedIndex != that.lastIncludedIndex)
            return false;
        if (lastIncludedTerm != that.lastIncludedTerm)
            return false;
        if (!peersList.equals(that.peersList))
            return false;
        if (!oldPeersList.equals(that.oldPeersList))
            return false;
        if (!learnersList.equals(that.learnersList))
            return false;
        return oldLearnersList.equals(that.oldLearnersList);
    }

    @Override public int hashCode() {
        int result = (int) (lastIncludedIndex ^ (lastIncludedIndex >>> 32));
        result = 31 * result + (int) (lastIncludedTerm ^ (lastIncludedTerm >>> 32));
        result = 31 * result + peersList.hashCode();
        result = 31 * result + oldPeersList.hashCode();
        result = 31 * result + learnersList.hashCode();
        result = 31 * result + oldLearnersList.hashCode();
        return result;
    }
}
