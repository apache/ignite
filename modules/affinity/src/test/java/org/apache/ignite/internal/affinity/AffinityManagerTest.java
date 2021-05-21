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

package org.apache.ignite.internal.affinity;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.client.Condition;
import org.apache.ignite.metastorage.client.Entry;
import org.apache.ignite.metastorage.client.EntryEvent;
import org.apache.ignite.metastorage.client.Operation;
import org.apache.ignite.metastorage.client.WatchEvent;
import org.apache.ignite.metastorage.client.WatchListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests scenarios for affinity manager.
 */
public class AffinityManagerTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AffinityManagerTest.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.assignment.";

    /** Node name. */
    public static final String NODE_NAME = "node1";

    /** The name of the table which is statically configured. */
    private static final String STATIC_TABLE_NAME = "t1";

    /** Configuration manager. */
    private ConfigurationManager cfrMgr;

    /** Before all test scenarios. */
    @BeforeEach
    private void before() {
        try {
            cfrMgr = new ConfigurationManager(rootConfigurationKeys(), Arrays.asList(
                new TestConfigurationStorage(ConfigurationType.DISTRIBUTED)));

            cfrMgr.bootstrap("{\n" +
                "   \"table\":{\n" +
                "      \"tables\":{\n" +
                "         \"" + STATIC_TABLE_NAME + "\":{\n" +
                "            \"name\":\"TestTable\",\n" +
                "            \"partitions\":16,\n" +
                "            \"replicas\":1,\n" +
                "            \"columns\":{\n" +
                "               \"id\":{\n" +
                "                  \"name\":\"id\",\n" +
                "                  \"type\":{\n" +
                "                     \"type\":\"Int64\"\n" +
                "                  },\n" +
                "                  \"nullable\":false\n" +
                "               }\n" +
                "            },\n" +
                "            \"indices\":{\n" +
                "               \"pk\":{\n" +
                "                  \"name\":\"pk\",\n" +
                "                  \"type\":\"primary\",\n" +
                "                  \"uniq\":true,\n" +
                "                  \"columns\":{\n" +
                "                     \"id\":{\n" +
                "                        \"name\":\"id\",\n" +
                "                        \"asc\":true\n" +
                "                     }\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   }\n" +
                "}", ConfigurationType.DISTRIBUTED);
        }
        catch (Exception e) {
            LOG.error("Failed to bootstrap the test configuration manager.", e);

            fail("Failed to configure manager [err=" + e.getMessage() + ']');
        }

    }

    /**
     * Gets a list of configuration keys to use in the test scenario.
     *
     * @return List of root configuration keys.
     */
    private static List<RootKey<?, ?>> rootConfigurationKeys() {
        return Arrays.asList(
            TablesConfiguration.KEY
        );
    }

    /**
     * The test calculates assignment by predefined table configuration and checks assignment calculated event.
     */
    @Test
    public void testCalculatedAssignment() {
        MetaStorageManager mm = mock(MetaStorageManager.class);
        BaselineManager bm = mock(BaselineManager.class);
        VaultManager vm = mock(VaultManager.class);

        UUID tblId = UUID.randomUUID();

        when(vm.get(any())).thenAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            assertEquals(INTERNAL_PREFIX + tblId, new String(key.bytes(), StandardCharsets.UTF_8));

            return CompletableFuture.completedFuture(new org.apache.ignite.internal.vault.common.Entry(key, STATIC_TABLE_NAME.getBytes(StandardCharsets.UTF_8)));
        });

        CompletableFuture<WatchListener> watchFut = new CompletableFuture<>();

        when(mm.registerWatchByPrefix(any(), any())).thenAnswer(invocation -> {
            ByteArray metastoreKeyPrefix = invocation.getArgument(0);

            assertEquals(INTERNAL_PREFIX, new String(metastoreKeyPrefix.bytes(), StandardCharsets.UTF_8));

            watchFut.complete(invocation.getArgument(1));

            return CompletableFuture.completedFuture(42L);
        });

        when(mm.invoke((Condition)any(), (Operation)any(), (Operation)any())).thenAnswer(invocation -> {
            assertTrue(watchFut.isDone());

            ByteArray key = new ByteArray(INTERNAL_PREFIX + tblId);

            Entry oldEntry = mock(Entry.class);

            when(oldEntry.key()).thenReturn(key);

            Entry newEntry = mock(Entry.class);

            when(newEntry.key()).thenReturn(key);
            when(newEntry.value()).thenReturn(ByteUtils.toBytes(Collections.EMPTY_LIST));

            WatchListener lsnr = watchFut.join();

            CompletableFuture.supplyAsync(() ->
                lsnr.onUpdate(new WatchEvent(new EntryEvent(oldEntry, newEntry))));

            return CompletableFuture.completedFuture(true);
        });

        AffinityManager affinityManager = new AffinityManager(cfrMgr, mm, bm, vm);

        CompletableFuture<Boolean> assignmentCalculated = new CompletableFuture<>();

        affinityManager.listen(AffinityEvent.CALCULATED, (parameters, e) -> assignmentCalculated.complete(e == null));

        affinityManager.calculateAssignments(tblId);

        assertTrue(assignmentCalculated.join());
    }

    /**
     * The test removes an assignment and checks assignment removed event.
     */
    @Test
    public void testRemovedAssignment() {
        MetaStorageManager mm = mock(MetaStorageManager.class);
        BaselineManager bm = mock(BaselineManager.class);
        VaultManager vm = mock(VaultManager.class);

        UUID tblId = UUID.randomUUID();

        CompletableFuture<WatchListener> watchFut = new CompletableFuture<>();

        when(mm.registerWatchByPrefix(any(), any())).thenAnswer(invocation -> {
            ByteArray metastoreKeyPrefix = invocation.getArgument(0);

            assertEquals(INTERNAL_PREFIX, new String(metastoreKeyPrefix.bytes(), StandardCharsets.UTF_8));

            watchFut.complete(invocation.getArgument(1));

            return CompletableFuture.completedFuture(42L);
        });

        when(mm.invoke((Condition)any(), (Operation)any(), (Operation)any())).thenAnswer(invocation -> {
            assertTrue(watchFut.isDone());

            ByteArray key = new ByteArray(INTERNAL_PREFIX + tblId);

            Entry oldEntry = mock(Entry.class);

            when(oldEntry.key()).thenReturn(key);
            when(oldEntry.value()).thenReturn(ByteUtils.toBytes(Collections.EMPTY_LIST));

            Entry newEntry = mock(Entry.class);

            when(newEntry.key()).thenReturn(key);

            WatchListener lsnr = watchFut.join();

            CompletableFuture.supplyAsync(() ->
                lsnr.onUpdate(new WatchEvent(new EntryEvent(oldEntry, newEntry))));

            return CompletableFuture.completedFuture(true);
        });

        AffinityManager affinityManager = new AffinityManager(cfrMgr, mm, bm, vm);

        CompletableFuture<Boolean> assignmentRemoved = new CompletableFuture<>();

        affinityManager.listen(AffinityEvent.REMOVED, (parameters, e) -> assignmentRemoved.complete(e == null));

        affinityManager.removeAssignment(tblId);

        assertTrue(assignmentRemoved.join());
    }
}
