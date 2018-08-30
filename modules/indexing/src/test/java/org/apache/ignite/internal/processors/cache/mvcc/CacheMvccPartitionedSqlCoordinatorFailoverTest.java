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

import org.apache.ignite.cache.CacheMode;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SCAN;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * SQL Mvcc coordinator failover test for partitioned caches.
 */
public class CacheMvccPartitionedSqlCoordinatorFailoverTest extends CacheMvccAbstractSqlCoordinatorFailoverTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_ClientServer_Backups2_CoordinatorFails() throws Exception {
        accountsTxReadAll(4, 2, 2, DFLT_PARTITION_COUNT,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_Server_Backups1_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(2, 0, 1, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups3_RestartCoordinator_ScanDml() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD  , 5, 2, 3, DFLT_PARTITION_COUNT,
            new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_RestartCoordinator_ScanDml_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD  , 2, 1, 2, DFLT_PARTITION_COUNT,
            new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups2_RestartCoordinator_SqlDml_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD, 4, 2, 2, 64,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_RestartCoordinator_SqlDml() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 2, 1, 1, 64,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_ClientServer_Backups2_Sql() throws Exception {
        updateNObjectsTest(7, 3, 2, 2, DFLT_PARTITION_COUNT, DFLT_TEST_TIME,
            new InitIndexing(Integer.class, Integer.class), SQL, DML, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_ClientServer_Backups1_Sql_Persistence() throws Exception {
        persistence = true;

        updateNObjectsTest(10, 2, 1, 1, DFLT_PARTITION_COUNT, DFLT_TEST_TIME,
            new InitIndexing(Integer.class, Integer.class), SQL, DML, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlReadInProgressCoordinatorFails() throws Exception {
        readInProgressCoordinatorFails(false, false, PESSIMISTIC, REPEATABLE_READ, SQL, DML, new InitIndexing(Integer.class, Integer.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlReadInsideTxInProgressCoordinatorFails() throws Exception {
        readInProgressCoordinatorFails(false, true, PESSIMISTIC, REPEATABLE_READ, SQL, DML, new InitIndexing(Integer.class, Integer.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlReadInProgressCoordinatorFails_ReadDelay() throws Exception {
        readInProgressCoordinatorFails(true, false, PESSIMISTIC, REPEATABLE_READ, SQL, DML, new InitIndexing(Integer.class, Integer.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlReadInsideTxInProgressCoordinatorFails_ReadDelay() throws Exception {
        readInProgressCoordinatorFails(true, true, PESSIMISTIC, REPEATABLE_READ, SQL, DML, new InitIndexing(Integer.class, Integer.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadInProgressCoordinatorFailsSimple_FromServer() throws Exception {
        readInProgressCoordinatorFailsSimple(false, new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }
}
