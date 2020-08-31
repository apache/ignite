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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SCAN;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Mvcc cache API coordinator failover test.
 */
public abstract class CacheMvccAbstractCoordinatorFailoverTest extends CacheMvccAbstractBasicCoordinatorFailoverTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxGet_Server_Backups0_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(2, 0, 0, 64,
            null, true, GET, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxGet_SingleNode_CoordinatorFails() throws Exception {
        accountsTxReadAll(1, 0, 0, 1,
            null, true, GET, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxScan_Server_Backups0_CoordinatorFails() throws Exception {
        accountsTxReadAll(2, 0, 0, 64,
            null, true, SCAN, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxScan_SingleNode_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(1, 0, 0, 1,
            null, true, SCAN, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_Server_Backups0_RestartCoordinator_GetPut() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 2, 0, 0, 64,
            null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_SingleNode_RestartCoordinator_GetPut_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD, 1, 0, 0, 1,
            null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate_N_Objects_Servers_Backups0__PutGet_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        updateNObjectsTest(5, 2, 0, 0, 64, DFLT_TEST_TIME,
            null, GET, PUT, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate_N_Objects_SingleNode__PutGet_CoordinatorFails() throws Exception {
        updateNObjectsTest(7, 1, 0, 0, 1, DFLT_TEST_TIME,
            null, GET, PUT, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorFailureSimplePessimisticTxPutGet() throws Exception {
        coordinatorFailureSimple(PESSIMISTIC, REPEATABLE_READ, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadInProgressCoordinatorFailsSimple_FromClientPutGet() throws Exception {
        readInProgressCoordinatorFailsSimple(true, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorChangeActiveQueryClientFails_Simple() throws Exception {
        checkCoordinatorChangeActiveQueryClientFails_Simple(null, GET, PUT);
    }

}
