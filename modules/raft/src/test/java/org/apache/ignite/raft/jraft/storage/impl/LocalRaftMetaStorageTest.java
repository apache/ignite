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
package org.apache.ignite.raft.jraft.storage.impl;

import java.io.File;
import java.io.IOException;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.RaftMetaStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
@ExtendWith(MockitoExtension.class)
public class LocalRaftMetaStorageTest extends BaseStorageTest {
    private RaftMetaStorage raftMetaStorage;

    @Mock
    private NodeImpl node;

    @Override
    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        this.raftMetaStorage = new LocalRaftMetaStorage(this.path, new RaftOptions());
        Mockito.when(this.node.getNodeMetrics()).thenReturn(null);
        assertTrue(this.raftMetaStorage.init(newOptions()));
    }

    private RaftMetaStorageOptions newOptions() {
        RaftMetaStorageOptions raftMetaStorageOptions = new RaftMetaStorageOptions();
        raftMetaStorageOptions.setNode(this.node);
        return raftMetaStorageOptions;
    }

    @Test
    public void testGetAndSetReload() {
        assertEquals(0, this.raftMetaStorage.getTerm());
        assertTrue(this.raftMetaStorage.getVotedFor().isEmpty());

        this.raftMetaStorage.setTerm(99);
        assertEquals(99, this.raftMetaStorage.getTerm());
        assertTrue(this.raftMetaStorage.getVotedFor().isEmpty());

        assertTrue(this.raftMetaStorage.setVotedFor(new PeerId("localhost", 8081)));
        assertEquals(99, this.raftMetaStorage.getTerm());
        assertEquals(new PeerId("localhost", 8081), this.raftMetaStorage.getVotedFor());

        assertTrue(this.raftMetaStorage.setTermAndVotedFor(100, new PeerId("localhost", 8083)));
        assertEquals(100, this.raftMetaStorage.getTerm());
        assertEquals(new PeerId("localhost", 8083), this.raftMetaStorage.getVotedFor());

        this.raftMetaStorage = new LocalRaftMetaStorage(this.path, new RaftOptions());
        Mockito.when(this.node.getNodeMetrics()).thenReturn(null);
        this.raftMetaStorage.init(newOptions());
        assertEquals(100, this.raftMetaStorage.getTerm());
        assertEquals(new PeerId("localhost", 8083), this.raftMetaStorage.getVotedFor());
    }

    @Test
    public void testSaveFail() throws IOException {
        Utils.delete(new File(this.path));
        assertFalse(this.raftMetaStorage.setVotedFor(new PeerId("localhost", 8081)));
        Mockito.verify(this.node, Mockito.times(1)).onError((RaftException) Mockito.any());
    }
}
