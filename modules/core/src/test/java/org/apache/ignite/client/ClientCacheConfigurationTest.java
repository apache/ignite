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

package org.apache.ignite.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

/**
 * {@link ClientConfiguration} unit tests.
 */
public class ClientCacheConfigurationTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /** Serialization/deserialization. */
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        ClientCacheConfiguration target = new ClientCacheConfiguration().setName("Person")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setEagerTtl(false)
            .setGroupName("FunctionalTest")
            .setDefaultLockTimeout(12345)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_ALL)
            .setReadFromBackup(true)
            .setRebalanceBatchSize(67890)
            .setRebalanceBatchesPrefetchCount(102938)
            .setRebalanceDelay(54321)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setRebalanceOrder(2)
            .setRebalanceThrottle(564738)
            .setRebalanceTimeout(142536)
            .setKeyConfiguration(new CacheKeyConfiguration("Employee", "orgId"))
            .setQueryEntities(new QueryEntity(int.class.getName(), "Employee")
                .setTableName("EMPLOYEE")
                .setFields(
                    Stream.of(
                        new SimpleEntry<>("id", Integer.class.getName()),
                        new SimpleEntry<>("orgId", Integer.class.getName())
                    ).collect(Collectors.toMap(
                        SimpleEntry::getKey, SimpleEntry::getValue, (a, b) -> a, LinkedHashMap::new
                    ))
                )
                .setKeyFields(Collections.singleton("id"))
                .setNotNullFields(Collections.singleton("id"))
                .setDefaultFieldValues(Collections.singletonMap("id", 0))
                .setIndexes(Collections.singletonList(new QueryIndex("id", true, "IDX_EMPLOYEE_ID")))
                .setAliases(Stream.of("id", "orgId").collect(Collectors.toMap(f -> f, String::toUpperCase)))
            );

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();

        ObjectOutput out = new ObjectOutputStream(outBytes);

        out.writeObject(target);
        out.flush();

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));

        Object desTarget = in.readObject();

        assertTrue(Comparers.equal(target, desTarget));
    }
}
