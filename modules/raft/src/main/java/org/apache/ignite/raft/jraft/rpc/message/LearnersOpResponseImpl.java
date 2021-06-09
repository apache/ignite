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
import org.apache.ignite.raft.jraft.rpc.CliRequests;

public class LearnersOpResponseImpl implements CliRequests.LearnersOpResponse, CliRequests.LearnersOpResponse.Builder {
    private List<String> oldLearnersList = new ArrayList<>();
    private List<String> newLearnersList = new ArrayList<>();

    @Override public List<String> getOldLearnersList() {
        return oldLearnersList;
    }

    @Override public int getOldLearnersCount() {
        return oldLearnersList.size();
    }

    @Override public String getOldLearners(int index) {
        return oldLearnersList.get(index);
    }

    @Override public List<String> getNewLearnersList() {
        return newLearnersList;
    }

    @Override public int getNewLearnersCount() {
        return newLearnersList.size();
    }

    @Override public String getNewLearners(int index) {
        return newLearnersList.get(index);
    }

    @Override public Builder addOldLearners(String oldLearnersId) {
        oldLearnersList.add(oldLearnersId);

        return this;
    }

    @Override public Builder addNewLearners(String newLearnersId) {
        newLearnersList.add(newLearnersId);

        return this;
    }

    @Override public CliRequests.LearnersOpResponse build() {
        return this;
    }
}
