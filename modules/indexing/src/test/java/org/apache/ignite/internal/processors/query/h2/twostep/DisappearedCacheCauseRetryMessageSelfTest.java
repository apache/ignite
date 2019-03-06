/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Organization;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Person;

/**
 * Failed to reserve partitions for query (cache is not found on local node) Root cause test
 */
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

        cfg.setCommunicationSpi(new TcpCommunicationSpi(){

            volatile long reqId = -1;
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                if ( GridIoMessage.class.isAssignableFrom(msg.getClass())){
                    GridIoMessage gridMsg = (GridIoMessage)msg;

                    if ( GridH2QueryRequest.class.isAssignableFrom( gridMsg.message().getClass() ) ){
                        GridH2QueryRequest req = (GridH2QueryRequest) (gridMsg.message());
                        reqId = req.requestId();
                        orgCache.destroy();
                    }
                    else if ( GridQueryCancelRequest.class.isAssignableFrom( gridMsg.message().getClass() ) ){
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
        System.setProperty(IGNITE_SQL_RETRY_TIMEOUT, "5000");

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
