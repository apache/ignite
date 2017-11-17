/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.trainers.group;

import java.util.UUID;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

public class TestGroupTrainerLocalContext implements HasTrainingUUID {
    private int cnt = 0;
    private int maxCnt;

    private int eachNumberCnt;
    private int limit;

    private UUID trainingUUID;

    public TestGroupTrainerLocalContext(int cnt, int eachNumberCnt, int limit, UUID trainingUUID) {
        this.cnt = cnt;
        this.eachNumberCnt = eachNumberCnt;
        this.limit = limit;
        this.trainingUUID = trainingUUID;
        maxCnt = 0;
    }

    public int cnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public TestGroupTrainerLocalContext incCnt() {
        this.cnt++;

        return this;
    }

    public int maxCnt() {
        return maxCnt;
    }

    public int eachNumberCnt() {
        return eachNumberCnt;
    }

    public int limit() {
        return limit;
    }

    @Override public UUID trainingUUID() {
        return trainingUUID;
    }
}
