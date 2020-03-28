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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Utility class for tests.
 */
public class GridCommandHandlerIndexingUtils {
    /** Test cache name. */
    public static final String CACHE_NAME = "persons-cache-vi";

    /** Test group name. */
    public static final String GROUP_NAME = "group1";

    /** Private constructor */
    private GridCommandHandlerIndexingUtils() {
        throw new IllegalArgumentException("don't create");
    }

    /**
     * Create and fill cache. Key - integer, value - {@code Person}.
     * <br/>
     * <table class="doctable">
     * <th>Cache parameter</th>
     * <th>Value</th>
     * <tr>
     *     <td>Synchronization mode</td>
     *     <td>{@link CacheWriteSynchronizationMode#FULL_SYNC FULL_SYNC}</td>
     * </tr>
     * <tr>
     *     <td>Atomicity mode</td>
     *     <td>{@link CacheAtomicityMode#ATOMIC ATOMIC}</td>
     * </tr>
     * <tr>
     *     <td>Number of backup</td>
     *     <td>1</td>
     * </tr>
     * <tr>
     *     <td>Query entities</td>
     *     <td>{@link #personEntity()}</td>
     * </tr>
     * <tr>
     *     <td>Affinity</td>
     *     <td>{@link RendezvousAffinityFunction} with exclNeighbors = false, parts = 32</td>
     * </tr>
     * </table>
     *
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @see Person
     * */
    public static void createAndFillCache(final Ignite ignite, final String cacheName, final String grpName) {
        assert nonNull(ignite);
        assert nonNull(cacheName);
        assert nonNull(grpName);

        ignite.createCache(new CacheConfiguration<Integer, Person>()
            .setName(cacheName)
            .setGroupName(grpName)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setQueryEntities(F.asList(personEntity()))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (IgniteDataStreamer<Integer, Person> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, new Person(rand.nextInt(), valueOf(rand.nextLong())));
        }
    }

    /**
     * Create query entity.
     */
    private static QueryEntity personEntity() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        String orgIdField = "orgId";
        String nameField = "name";

        entity.addQueryField(orgIdField, Integer.class.getName(), null);
        entity.addQueryField(nameField, String.class.getName(), null);

        entity.setIndexes(asList(new QueryIndex(nameField), new QueryIndex(orgIdField)));

        return entity;
    }

    /**
     * Simple class for tests.
     */
    static class Person implements Serializable {
        /** Id organization. */
        int orgId;

        /** Name organization. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
