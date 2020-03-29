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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageVersion.INITIAL_VERSION;
import static org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorker.DUMMY_VALUE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/** */
public class InMemoryCachedDistributedMetaStorageBridgeTest {
    /** */
    private JdkMarshaller marshaller;

    /** */
    private InMemoryCachedDistributedMetaStorageBridge bridge;

    /** */
    @Before
    public void before() {
        marshaller = JdkMarshaller.DEFAULT;

        bridge = new InMemoryCachedDistributedMetaStorageBridge(marshaller);
    }

    /** */
    @Test
    public void testReadWrite() throws Exception {
        byte[] valBytes = marshaller.marshal("value");

        bridge.write("key", valBytes);

        assertEquals("value", bridge.read("key"));

        assertArrayEquals(valBytes, bridge.readMarshalled("key"));
    }

    /** */
    @Test
    public void testIterate() throws Exception {
        bridge.write("key3", marshaller.marshal("val3"));
        bridge.write("key1", marshaller.marshal("val1"));
        bridge.write("key2", marshaller.marshal("val2"));
        bridge.write("xey4", marshaller.marshal("val4"));

        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();

        bridge.iterate("key", (key, value) -> {
            assertThat(value, is(instanceOf(String.class)));

            keys.add(key);
            values.add((String)value);
        });

        assertEquals(asList("key1", "key2", "key3"), keys);
        assertEquals(asList("val1", "val2", "val3"), values);
    }

    /** */
    @Test
    public void testLocalFullData() throws Exception {
        byte[] valBytes1 = marshaller.marshal("val1");
        byte[] valBytes2 = marshaller.marshal("val2");
        byte[] valBytes3 = marshaller.marshal("val3");

        bridge.write("key3", valBytes3);
        bridge.write("key2", valBytes2);
        bridge.write("key1", valBytes1);

        DistributedMetaStorageKeyValuePair[] exp = {
            new DistributedMetaStorageKeyValuePair("key1", valBytes1),
            new DistributedMetaStorageKeyValuePair("key2", valBytes2),
            new DistributedMetaStorageKeyValuePair("key3", valBytes3)
        };

        assertArrayEquals(exp, bridge.localFullData());
    }

    /** */
    @Test
    public void testWriteFullNodeData() throws Exception {
        bridge.write("oldKey", marshaller.marshal("oldVal"));

        bridge.writeFullNodeData(new DistributedMetaStorageClusterNodeData(
            DistributedMetaStorageVersion.INITIAL_VERSION,
            new DistributedMetaStorageKeyValuePair[] {
                new DistributedMetaStorageKeyValuePair("newKey", marshaller.marshal("newVal"))
            },
            DistributedMetaStorageHistoryItem.EMPTY_ARRAY,
            null
        ));

        assertNull(bridge.read("oldKey"));
        assertEquals("newVal", bridge.read("newKey"));
    }

    /** */
    @Test
    public void testReadInitialDataAfterFailedCleanup() throws Exception {
        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();

        metastorage.write(cleanupGuardKey(), DUMMY_VALUE);

        metastorage.write(COMMON_KEY_PREFIX + "dummy1", "val1");
        metastorage.write(COMMON_KEY_PREFIX + "dummy2", "val2");

        bridge.readInitialData(metastorage);

        assertArrayEquals(DistributedMetaStorageKeyValuePair.EMPTY_ARRAY, bridge.localFullData());
    }

    /** */
    @Test
    public void testReadInitialData1() throws Exception {
        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();

        metastorage.write(versionKey(), INITIAL_VERSION);

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        metastorage.write(historyItemKey(1), histItem);

        bridge.readInitialData(metastorage);

        assertEquals(1, bridge.localFullData().length);

        assertEquals("val1", bridge.read("key1"));
    }

    /** */
    @Test
    public void testReadInitialData2() throws Exception {
        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();

        metastorage.write(versionKey(), INITIAL_VERSION);

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        metastorage.write(historyItemKey(1), histItem);
        metastorage.write(versionKey(), INITIAL_VERSION.nextVersion(histItem));

        bridge.readInitialData(metastorage);

        assertEquals(1, bridge.localFullData().length);

        assertEquals("val1", bridge.read("key1"));
    }

    /** */
    @Test
    public void testReadInitialData3() throws Exception {
        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();

        metastorage.write(versionKey(), INITIAL_VERSION);

        DistributedMetaStorageHistoryItem histItem = histItem("key1", "val1");

        metastorage.write(historyItemKey(1), histItem);
        metastorage.write(versionKey(), INITIAL_VERSION.nextVersion(histItem));
        metastorage.write(localKey("key1"), "wrongValue");

        bridge.readInitialData(metastorage);

        assertEquals(1, bridge.localFullData().length);

        assertEquals("val1", bridge.read("key1"));
    }

    /** */
    private DistributedMetaStorageHistoryItem histItem(String key, String val) throws IgniteCheckedException {
        return new DistributedMetaStorageHistoryItem(key, marshaller.marshal(val));
    }
}
