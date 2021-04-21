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

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Organization;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Person;

/**
 * Failed to reserve partitions for query (cache is not found on local node) Root cause test
 */
@WithSystemProperty(key = IGNITE_SQL_RETRY_TIMEOUT, value = "5000")
public class DisappearedCacheCauseRetryMessageSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final String ORG = "org";

    /** */
    private IgniteCache<String, JoinSqlTestHelper.Person> personCache;

    /** */
    private IgniteCache<String, JoinSqlTestHelper.Organization> orgCache;

    /** */
    @Test
    public void testDisappearedCacheCauseRetryMessage() {
        SqlQuery<String, JoinSqlTestHelper.Person> qry =
            new SqlQuery<String, JoinSqlTestHelper.Person>(JoinSqlTestHelper.Person.class, JoinSqlTestHelper.JOIN_SQL)
                .setArgs("Organization #0");

        qry.setDistributedJoins(true);

        try {
            personCache.query(qry).getAll();

            fail("No CacheException emitted.");
        }
        catch (CacheException e) {
            if (!e.getMessage().contains("Failed to reserve partitions for query (cache is not found on local node) ["))
                e.printStackTrace();

            assertTrue(e.getMessage(), e.getMessage().contains("Failed to reserve partitions for query (cache is not found on local node) ["));
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {

            volatile long reqId = -1;
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                if (GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                    GridIoMessage gridMsg = (GridIoMessage)msg;

                    if (GridH2QueryRequest.class.isAssignableFrom(gridMsg.message().getClass())) {
                        GridH2QueryRequest req = (GridH2QueryRequest)(gridMsg.message());
                        reqId = req.requestId();
                        orgCache.destroy();
                    }
                    else if (GridQueryCancelRequest.class.isAssignableFrom(gridMsg.message().getClass())) {
                        GridQueryCancelRequest req = (GridQueryCancelRequest) (gridMsg.message());

                        if (reqId == req.queryRequestId())
                            orgCache = DisappearedCacheCauseRetryMessageSelfTest.this.ignite(0)
                                .getOrCreateCache(new CacheConfiguration<String, Organization>(ORG)
                                .setCacheMode(CacheMode.REPLICATED)
                                .setQueryEntities(JoinSqlTestHelper.organizationQueryEntity())
                            );
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);

        personCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Person>("pers")
            .setQueryEntities(JoinSqlTestHelper.personQueryEntity())
        );

        orgCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Organization>(ORG)
            .setCacheMode(CacheMode.REPLICATED)
            .setQueryEntities(JoinSqlTestHelper.organizationQueryEntity())
        );

        awaitPartitionMapExchange();

        JoinSqlTestHelper.populateDataIntoOrg(orgCache);

        JoinSqlTestHelper.populateDataIntoPerson(personCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
