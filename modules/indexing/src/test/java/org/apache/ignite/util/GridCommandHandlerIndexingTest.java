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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;

/**
 * If you not necessary create nodes for each test you can try use
 * {@link GridCommandHandlerIndexingClusterByClassTest}.
 */
public class GridCommandHandlerIndexingTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    @Test
    public void testValidateIndexesFailedOnNotIdleCluster() throws Exception {
        checkpointFreq = 100L;

        Ignite ignite = prepareGridForTest();

        AtomicBoolean stopFlag = new AtomicBoolean();

        IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> cache = ignite.cache(CACHE_NAME);

        Thread loadThread = new Thread(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopFlag.get()) {
                int id = rnd.nextInt();

                cache.put(id, new GridCommandHandlerIndexingUtils.Person(id, "name" + id));

                if (Thread.interrupted())
                    break;
            }
        });

        try {
            loadThread.start();

            doSleep(checkpointFreq);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));
        }
        finally {
            stopFlag.set(true);

            loadThread.join();
        }

        String out = testOut.toString();

        assertContains(log, out, "Index validation failed");
        assertContains(log, out, "Checkpoint with dirty pages started! Cluster not idle!");
    }

    /**
     * Tests that corrupted pages in the index partition are detected.
     */
    @Test
    public void testCorruptedIndexPartitionShouldFailValidation() throws Exception {
        Ignite ignite = prepareGridForTest();

        forceCheckpoint();

        File idxPath = indexPartition(ignite, GROUP_NAME);

        stopAllGrids();

        corruptIndexPartition(idxPath);

        startGrids(2);

        awaitPartitionMapExchange();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertContains(log, testOut.toString(), "issues found (listed above)");
    }

    /**
     * Create and fill nodes.
     *
     * @throws Exception
     */
    private Ignite prepareGridForTest() throws Exception{
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid(CLIENT_NODE_NAME_PREFIX);

        createAndFillCache(client, CACHE_NAME, GROUP_NAME);

        return ignite;
    }

    /**
     * Get index partition file for specific node and cache.
     */
    private File indexPartition(Ignite ig, String groupName) {
        IgniteEx ig0 = (IgniteEx)ig;

        FilePageStoreManager pageStoreManager = ((FilePageStoreManager) ig0.context().cache().context().pageStore());

        return new File(pageStoreManager.cacheWorkDir(true, groupName), INDEX_FILE_NAME);
    }

    /**
     * Write some random trash in index partition.
     */
    private void corruptIndexPartition(File path) throws IOException {
        assertTrue(path.exists());

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (RandomAccessFile idx = new RandomAccessFile(path, "rw")) {
            byte[] trash = new byte[1024];

            rand.nextBytes(trash);

            idx.seek(4096);

            idx.write(trash);
        }
    }
}
