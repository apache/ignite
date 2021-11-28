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

package org.apache.ignite.internal.tx;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Basic tests for a transaction manager.
 */
@ExtendWith(MockitoExtension.class)
public class TxManagerTest extends IgniteAbstractTest {
    private static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    private TxManager txManager;

    @Mock
    private ClusterService clusterService;

    /** Init test callback. */
    @BeforeEach
    public void before() {
        clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);

        Mockito.when(clusterService.topologyService().localMember().address()).thenReturn(ADDR);

        txManager = new TxManagerImpl(clusterService, new HeapLockManager());
    }

    @Test
    public void testBegin() throws TransactionException {
        InternalTransaction tx = txManager.begin();

        assertNotNull(tx.timestamp());
        assertEquals(TxState.PENDING, txManager.begin().state());
    }

    @Test
    public void testCommit() throws TransactionException {
        InternalTransaction tx = txManager.begin();
        tx.commit();

        assertEquals(TxState.COMMITED, tx.state());
        assertEquals(TxState.COMMITED, txManager.state(tx.timestamp()));

        assertThrows(TransactionException.class, () -> tx.rollback());

        assertEquals(TxState.COMMITED, tx.state());
        assertEquals(TxState.COMMITED, txManager.state(tx.timestamp()));
    }

    @Test
    public void testRollback() throws TransactionException {
        InternalTransaction tx = txManager.begin();
        tx.rollback();

        assertEquals(TxState.ABORTED, tx.state());
        assertEquals(TxState.ABORTED, txManager.state(tx.timestamp()));

        assertThrows(TransactionException.class, () -> tx.commit());

        assertEquals(TxState.ABORTED, tx.state());
        assertEquals(TxState.ABORTED, txManager.state(tx.timestamp()));
    }

    @Test
    public void testForget() throws TransactionException {
        InternalTransaction tx = txManager.begin();

        assertEquals(TxState.PENDING, tx.state());

        txManager.forget(tx.timestamp());

        assertNull(tx.state());
    }

    @Test
    public void testEnlist() throws TransactionException {
        NetworkAddress addr = clusterService.topologyService().localMember().address();

        assertEquals(ADDR, addr);

        InternalTransaction tx = txManager.begin();

        RaftGroupService svc = Mockito.mock(RaftGroupService.class);

        tx.enlist(svc);

        assertEquals(1, tx.enlisted().size());
        assertTrue(tx.enlisted().contains(svc));
    }

    @Test
    public void testTimestamp() throws InterruptedException {
        Timestamp ts1 = Timestamp.nextVersion();
        Timestamp ts2 = Timestamp.nextVersion();
        Timestamp ts3 = Timestamp.nextVersion();

        Thread.sleep(1);

        Timestamp ts4 = Timestamp.nextVersion();

        assertTrue(ts2.compareTo(ts1) > 0);
        assertTrue(ts3.compareTo(ts2) > 0);
        assertTrue(ts4.compareTo(ts3) > 0);
    }
}
