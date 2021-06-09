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
package org.apache.ignite.raft.jraft.core;

/**
 * Node state
 */
public enum State {
    STATE_LEADER, // It's a leader
    STATE_TRANSFERRING, // It's transferring leadership
    STATE_CANDIDATE, //  It's a candidate
    STATE_FOLLOWER, // It's a follower
    STATE_ERROR, // It's in error
    STATE_UNINITIALIZED, // It's uninitialized
    STATE_SHUTTING, // It's shutting down
    STATE_SHUTDOWN, // It's shutdown already
    STATE_END; // State end

    public boolean isActive() {
        return this.ordinal() < STATE_ERROR.ordinal();
    }
}
