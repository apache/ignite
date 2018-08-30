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

import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SCAN;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Mvcc SQL API coordinator failover test.
 */
@SuppressWarnings("unchecked")
public abstract class CacheMvccAbstractSqlCoordinatorFailoverTest extends CacheMvccAbstractBasicCoordinatorFailoverTest {
    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_Server_Backups0_CoordinatorFails() throws Exception {
        accountsTxReadAll(2, 1, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(1, 0, 0, 1,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0_RestartCoordinator_ScanDml() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD  , 2, 1, 0, 64,
            new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_RestartCoordinator_ScanDml_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD  , 1, 0, 0, 1,
            new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0_RestartCoordinator_SqlDml() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 2, 1, 0, DFLT_PARTITION_COUNT,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_RestartCoordinator_SqlDml_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD, 1, 0, 0, 1,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_ClientServer_Backups0_Sql_Persistence() throws Exception {
        persistence = true;

        updateNObjectsTest(5, 2, 0, 0, 64, DFLT_TEST_TIME,
            new InitIndexing(Integer.class, Integer.class), SQL, DML, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode_Sql_Persistence() throws Exception {
        updateNObjectsTest(3, 1, 0, 0, 1, DFLT_TEST_TIME,
            new InitIndexing(Integer.class, Integer.class), SQL, DML, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCoordinatorFailureSimplePessimisticTxSql() throws Exception {
        coordinatorFailureSimple(PESSIMISTIC, REPEATABLE_READ, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxInProgressCoordinatorChangeSimple_Readonly() throws Exception {
        txInProgressCoordinatorChangeSimple(PESSIMISTIC, REPEATABLE_READ,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadInProgressCoordinatorFailsSimple_FromClient() throws Exception {
        readInProgressCoordinatorFailsSimple(true, new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCoordinatorChangeActiveQueryClientFails_Simple() throws Exception {
        checkCoordinatorChangeActiveQueryClientFails_Simple(new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCoordinatorChangeActiveQueryClientFails_SimpleScan() throws Exception {
        checkCoordinatorChangeActiveQueryClientFails_Simple(new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }
}
