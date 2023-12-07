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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.junit.Test;

/**
 * Tests distributed queries over set of partitions on stable topology.
 */
public class IgniteCacheDistributedPartitionQuerySelfTest extends IgniteCacheDistributedPartitionQueryAbstractSelfTest {
    /** Tests query within region. */
    @Test
    public void testRegionQuery() {
        doTestRegionQuery(grid(0));
    }

    /** Tests query within region (client). */
    @Test
    public void testRegionQueryClient() throws Exception {
        doTestRegionQuery(grid("client"));
    }

    /** Test query within partitions. */
    @Test
    public void testPartitionsQuery() {
        doTestPartitionsQuery(grid(0));
    }

    /** Test query within partitions (client). */
    @Test
    public void testPartitionsQueryClient() throws Exception {
        doTestPartitionsQuery(grid("client"));
    }

    /** Tests join query within region. */
    @Test
    public void testJoinQuery() {
        doTestJoinQuery(grid(0));
    }

    /** Tests join query within region. */
    @Test
    public void testJoinQueryClient() throws Exception {
        doTestJoinQuery(grid("client"));
    }

    /** Tests local query over partitions. */
    @Test
    public void testLocalQuery() {
        Affinity<Object> affinity = grid(0).affinity("cl");

        int[] parts = affinity.primaryPartitions(grid(0).localNode());

        Arrays.sort(parts);

        IgniteCache<ClientKey, Client> cl = grid(0).cache("cl");

        SqlQuery<ClientKey, Client> qry1 = new SqlQuery<>(Client.class, "1=1");
        qry1.setLocal(true);
        qry1.setPartitions(parts[0]);

        List<Cache.Entry<ClientKey, Client>> clients = cl.query(qry1).getAll();

        for (Cache.Entry<ClientKey, Client> client : clients)
            assertEquals("Incorrect partition", parts[0], affinity.partition(client.getKey()));

        SqlFieldsQuery qry2 = new SqlFieldsQuery("select cl._KEY, cl._VAL from \"cl\".Client cl");
        qry2.setLocal(true);
        qry2.setPartitions(parts[0]);

        List<List<?>> rows = cl.query(qry2).getAll();

        for (List<?> row : rows)
            assertEquals("Incorrect partition", parts[0], affinity.partition(row.get(0)));
    }
}
