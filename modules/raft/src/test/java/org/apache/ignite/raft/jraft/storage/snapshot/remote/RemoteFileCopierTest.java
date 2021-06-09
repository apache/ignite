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
package org.apache.ignite.raft.jraft.storage.snapshot.remote;

import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class RemoteFileCopierTest {
    private RemoteFileCopier copier;
    @Mock
    private RaftClientService rpcService;
    private TimerManager timerManager;

    @Before
    public void setup() {
        this.timerManager = new TimerManager(5);
        copier = new RemoteFileCopier();
    }

    @Test
    public void testInit() {
        Mockito.when(rpcService.connect(new Endpoint("localhost", 8081))).thenReturn(true);
        assertTrue(copier.init("remote://localhost:8081/999", null, new SnapshotCopierOptions(rpcService, timerManager,
            new RaftOptions(), new NodeOptions())));
        assertEquals(999, copier.getReaderId());
        Assert.assertEquals("localhost", copier.getEndpoint().getIp());
        Assert.assertEquals(8081, copier.getEndpoint().getPort());
    }

    @Test
    public void testInitFail() {
        Mockito.when(rpcService.connect(new Endpoint("localhost", 8081))).thenReturn(false);
        assertFalse(copier.init("remote://localhost:8081/999", null, new SnapshotCopierOptions(rpcService,
            timerManager, new RaftOptions(), new NodeOptions())));
    }
}
