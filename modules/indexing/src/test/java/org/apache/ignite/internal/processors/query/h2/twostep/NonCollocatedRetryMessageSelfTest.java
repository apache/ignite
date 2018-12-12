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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.List;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;

/**
 * Failed to execute non-collocated query root cause message test
 */
public class NonCollocatedRetryMessageSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final String ORG = "org";

    /** */
    private static final int TEST_SQL_RETRY_TIMEOUT = 500;

    /** */
    private String sqlRetryTimeoutBackup;

    /** */
    private IgniteCache<String, JoinSqlTestHelper.Person> personCache;

    /** */
    public void testNonCollocatedRetryMessage() {
        SqlQuery<String, JoinSqlTestHelper.Person> qry = new SqlQuery<String, JoinSqlTestHelper.Person>(
            JoinSqlTestHelper.Person.class, JoinSqlTestHelper.JOIN_SQL).setArgs("Organization #0");

        qry.setDistributedJoins(true);

        try {
            List<Cache.Entry<String, JoinSqlTestHelper.Person>> prsns = personCache.query(qry).getAll();

            fail("No CacheException emitted. Collection size=" + prsns.size());
        }
        catch (CacheException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Failed to execute non-collocated query"));
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestTcpCommunication());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sqlRetryTimeoutBackup = System.getProperty(IGNITE_SQL_RETRY_TIMEOUT);

        System.setProperty(IGNITE_SQL_RETRY_TIMEOUT, String.valueOf(TEST_SQL_RETRY_TIMEOUT));

        startGridsMultiThreaded(NODES_COUNT, false);

        CacheConfiguration<String, JoinSqlTestHelper.Person> ccfg1 = new CacheConfiguration<>("pers");

        ccfg1.setBackups(1);
        ccfg1.setQueryEntities(JoinSqlTestHelper.personQueryEntity());

        personCache = ignite(0).getOrCreateCache(ccfg1);

        CacheConfiguration<String, JoinSqlTestHelper.Organization> ccfg2 = new CacheConfiguration<>(ORG);

        ccfg2.setBackups(1);
        ccfg2.setQueryEntities(JoinSqlTestHelper.organizationQueryEntity());

        IgniteCache<String, JoinSqlTestHelper.Organization> orgCache = ignite(0).getOrCreateCache(ccfg2);

        awaitPartitionMapExchange();

        JoinSqlTestHelper.populateDataIntoOrg(orgCache);

        JoinSqlTestHelper.populateDataIntoPerson(personCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        if (sqlRetryTimeoutBackup != null)
            System.setProperty(IGNITE_SQL_RETRY_TIMEOUT, sqlRetryTimeoutBackup);

        stopAllGrids();
    }

    /**
     * TcpCommunicationSpi with additional features needed for tests.
     */
    private class TestTcpCommunication extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            assert msg != null;

            if (igniteInstanceName.equals(getTestIgniteInstanceName(1)) &&
                GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                GridIoMessage gridMsg = (GridIoMessage)msg;

                if (GridH2IndexRangeRequest.class.isAssignableFrom(gridMsg.message().getClass())) {
                    try {
                        U.sleep(TEST_SQL_RETRY_TIMEOUT);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        fail("Test was interrupted.");
                    }

                    throw new IgniteSpiException("Test exception.");
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}

