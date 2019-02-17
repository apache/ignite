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

package org.apache.ignite.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/**
 *
 */
public class GridCommandHandlerIndexingTest extends GridCommandHandlerTest {
    /**
     *
     */
    public void testValidateIndexes() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        IgniteCache<Integer, Person> personCache = client.getOrCreateCache(new CacheConfiguration<Integer, Person>()
            .setName("persons-cache-vi")
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setQueryEntities(F.asList(personEntity(true, true)))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (int i = 0; i < 1000; i++)
            personCache.put(i, new Person(rand.nextInt(), String.valueOf(rand.nextLong())));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "persons-cache-vi"));

        assertTrue(testOut.toString().contains("validate_indexes has finished, no issues found"));
    }

    /**
     * @param idxName Index name.
     * @param idxOrgId Index org id.
     */
    private QueryEntity personEntity(boolean idxName, boolean idxOrgId) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        entity.addQueryField("orgId", Integer.class.getName(), null);
        entity.addQueryField("name", String.class.getName(), null);

        List<QueryIndex> idxs = new ArrayList<>();

        if (idxName) {
            QueryIndex idx = new QueryIndex("name");

            idxs.add(idx);
        }

        if (idxOrgId) {
            QueryIndex idx = new QueryIndex("orgId");

            idxs.add(idx);
        }

        entity.setIndexes(idxs);

        return entity;
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        int orgId;

        /** */
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
