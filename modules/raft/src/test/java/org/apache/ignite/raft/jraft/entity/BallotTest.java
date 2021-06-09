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

import org.apache.ignite.raft.jraft.JRaftUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BallotTest {

    private Ballot ballot;

    @Before
    public void setup() {
        this.ballot = new Ballot();
        this.ballot.init(JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083"), null);
    }

    @Test
    public void testGrant() {
        PeerId peer1 = new PeerId("localhost", 8081);
        this.ballot.grant(peer1);
        assertFalse(this.ballot.isGranted());

        PeerId unfoundPeer = new PeerId("localhost", 8084);
        this.ballot.grant(unfoundPeer);
        assertFalse(this.ballot.isGranted());

        PeerId peer2 = new PeerId("localhost", 8082);
        this.ballot.grant(peer2);
        assertTrue(this.ballot.isGranted());
    }
}
