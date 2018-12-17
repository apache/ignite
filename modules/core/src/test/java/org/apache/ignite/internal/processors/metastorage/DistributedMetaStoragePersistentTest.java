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

package org.apache.ignite.internal.processors.metastorage;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;

/** */
@RunWith(JUnit4.class)
public class DistributedMetaStoragePersistentTest extends DistributedMetaStorageTest {
    /** {@inheritDoc} */
    @Override protected boolean isPersistent() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRestart() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        assertEquals("value", ignite.context().globalMetastorage().read("key"));
    }

    /** */
    @Test
    public void testJoinDirtyNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key1", "value1");

        Thread.sleep(150L); // Remove later.

        stopGrid(1);

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key2", "value2");

        Thread.sleep(150L); // Remove later.

        IgniteEx newNode = startGrid(1);

        assertEquals("value1", newNode.context().globalMetastorage().read("key1"));

        assertEquals("value2", newNode.context().globalMetastorage().read("key2"));
    }

    /** */
    @Test
    public void testJoinDirtyNodeFullData() throws Exception {
        System.setProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, "0");

        try {

            IgniteEx ignite = startGrid(0);

            startGrid(1);

            ignite.cluster().active(true);

            ignite.context().globalMetastorage().write("key1", "value1");

            Thread.sleep(150L); // Remove later.

            stopGrid(1);

            stopGrid(0);

            ignite = startGrid(0);

            ignite.cluster().active(true);

            ignite.context().globalMetastorage().write("key2", "value2");

            ignite.context().globalMetastorage().write("key3", "value3");

            Thread.sleep(150L); // Remove later.

            IgniteEx newNode = startGrid(1);

            assertEquals("value1", newNode.context().globalMetastorage().read("key1"));

            assertEquals("value2", newNode.context().globalMetastorage().read("key2"));

            assertEquals("value3", newNode.context().globalMetastorage().read("key3"));
        }
        finally {
            System.clearProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES);
        }
    }

    /** */
    @Test
    public void testJoinNodeWithLongerHistory() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key1", "value1");

        Thread.sleep(150L); // Remove later.

        stopGrid(1);

        ignite.context().globalMetastorage().write("key2", "value2");

        Thread.sleep(150L); // Remove later.

        stopGrid(0);

        ignite = startGrid(1);

        assertEquals("value1", ignite.context().globalMetastorage().read("key1"));

        assertNull(ignite.context().globalMetastorage().read("key2"));

        startGrid(0);

        awaitPartitionMapExchange();

        assertEquals("value1", ignite.context().globalMetastorage().read("key1"));

        assertEquals("value2", ignite.context().globalMetastorage().read("key2"));
    }

    /** */
    @Test
    public void testNamesCollision() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCacheDatabaseSharedManager dbSharedMgr = ignite.context().cache().context().database();

        MetaStorage locMetastorage = dbSharedMgr.metaStorage();

        DistributedMetaStorage globalMetastorage = ignite.context().globalMetastorage();

        dbSharedMgr.checkpointReadLock();

        try {
            locMetastorage.write("key", "localValue");
        }
        finally {
            dbSharedMgr.checkpointReadUnlock();
        }

        globalMetastorage.write("key", "globalValue");

        Thread.sleep(150L); // Remove later.

        dbSharedMgr.checkpointReadLock();

        try {
            assertEquals("localValue", locMetastorage.read("key"));
        }
        finally {
            dbSharedMgr.checkpointReadUnlock();
        }

        assertEquals("globalValue", globalMetastorage.read("key"));
    }
}
