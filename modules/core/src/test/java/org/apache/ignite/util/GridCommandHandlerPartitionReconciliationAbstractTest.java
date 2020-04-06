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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationRepairMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.io.File.separatorChar;
import static org.apache.ignite.TestStorageUtils.corruptDataEntry;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;

/**
 * Tests for checking partition reconciliation control.sh command.
 */
public abstract class GridCommandHandlerPartitionReconciliationAbstractTest extends
    GridCommandHandlerClusterPerMethodAbstractTest {
    /**
     *
     */
    public static final int INVALID_KEY = 100;

    /**
     *
     */
    public static final String VALUE_PREFIX = "abc_";

    /**
     *
     */
    public static final String BROKEN_POSTFIX_1 = "_broken1";

    /**
     *
     */
    public static final String BROKEN_POSTFIX_2 = "_broken2";

    /**
     *
     */
    protected static File dfltDiagnosticDir;

    /**
     *
     */
    protected static File customDiagnosticDir;

    /**
     *
     */
    protected IgniteEx ignite;

    /**
     *
     */
    private static CommandHandler hnd;

    /**
     * <ul>
     * <li>Init diagnostic and persistence dirs.</li>
     * <li>Start 4 nodes and activate cluster.</li>
     * <li>Prepare cache.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        initDiagnosticDir();

        cleanDiagnosticDir();

        cleanPersistenceDir();

        ignite = startGrids(4);

        ignite.cluster().active(true);

        prepareCache();

        hnd = new CommandHandler();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Do nothing;
    }

    /**
     * Create cache and populate it with some data.
     */
    protected abstract void prepareCache();

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        cleanDiagnosticDir();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDiagnosticDir() throws IgniteCheckedException {
        dfltDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + DEFAULT_TARGET_FOLDER + separatorChar);

        customDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + "diagnostic_test_dir" + separatorChar);
    }

    /**
     * Clean diagnostic directories.
     */
    protected void cleanDiagnosticDir() {
        U.delete(dfltDiagnosticDir);
        U.delete(customDiagnosticDir);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * (PRINT_ONLY in given case) and also contains the value that was used for fix (val1 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes has same value that is equal to val1
     * in given case.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemovedEntryOnPrimaryWithDefaultRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(true);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateResult(
            invalidDataRowMeta,
            hnd.getLastOperationResult(),
            new PartitionReconciliationRepairMeta(
                true,
                null,
                RepairAlgorithm.PRINT_ONLY));
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>MAJORITY</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * (MAJORITY in given case) and also contains the value that was used for fix (val1 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes has same value that is equal to val1
     * in given case.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemovedEntryOnPrimaryWithMajorityRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(true);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "MAJORITY", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(), VALUE_PREFIX + INVALID_KEY,
            RepairAlgorithm.MAJORITY);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>REMOVE</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, doesn't contain with key.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemovedEntryOnPrimaryWithRemoveRepairAlg() throws IgniteCheckedException {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(true);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "REMOVE", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was removed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(), null,
            RepairAlgorithm.REMOVE);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>PRIMARY</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * (PRIMARY in given case) and also contains the value that was used for fix (null in given case).</li>
     * <li>Ensure that previously inconsistent key was removed on all 4 nodes.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemovedEntryOnPrimaryWithPrimaryRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(true);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "PRIMARY", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result.
        validateResult(
            invalidDataRowMeta,
            hnd.getLastOperationResult(),
            new PartitionReconciliationRepairMeta(
                true,
                null,
                RepairAlgorithm.PRIMARY));

        // Enusre that invalid key was successfully fixed.
        for (int i = 0; i < 4; i++) {
            Object val = ignite(i).cachex(DEFAULT_CACHE_NAME).localPeek(INVALID_KEY, null, null);

            assertNull(val);
        }
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>LATEST</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * (LATEST in given case) and also contains the value that was used for fix (val3 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes has same value that is equal to val3
     * in given case.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemovedEntryOnPrimaryWithMaxGridCacheVersionRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(true);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "LATEST", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(),
            VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            RepairAlgorithm.LATEST);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>PRINT_ONLY</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary: key is missing</li>
     * <li>Backup 1: key has ver1  and val1</li>
     * <li>Backup 2: key has ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * (PRINT_ONLY in given case) and doesn't contain the value that was used for fix.</li>
     * <li>Ensure that no repair was applied.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemovedEntryOnPrimaryWithPrintOnlyRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(true);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "--recheck-delay", "0", "--local-output"));

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        // Validate partition reconciliation result.
        validateResult(
            invalidDataRowMeta,
            hnd.getLastOperationResult(),
            new PartitionReconciliationRepairMeta(
                true,
                null,
                RepairAlgorithm.PRINT_ONLY));

        // Enusre that invalid key wasn't fixed.
        assertNull(((IgniteEx)grid(nodes.get(0))).cachex(DEFAULT_CACHE_NAME).localPeek(INVALID_KEY, null, null));

        assertEquals(VALUE_PREFIX + INVALID_KEY,
            ((IgniteEx)grid(nodes.get(1))).cachex(DEFAULT_CACHE_NAME).localPeek(INVALID_KEY, null, null));

        assertEquals(VALUE_PREFIX + INVALID_KEY,
            ((IgniteEx)grid(nodes.get(2))).cachex(DEFAULT_CACHE_NAME).localPeek(INVALID_KEY, null, null));

        assertEquals(VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            ((IgniteEx)grid(nodes.get(3))).cachex(DEFAULT_CACHE_NAME).localPeek(INVALID_KEY, null, null));
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode.</li>
     * <p>
     * <b>Expected result:</b>
     * Cause there are no missing keys: neither totally missing, nor available only in deferred delete queue, default
     * MAJORITY algorithm won't be used, LATEST will be used instead.
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * LATEST in given case algorithm and also contains the value that was used for fix (val3 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes  has same value that is equal to val3.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMissedUpdateWithDefaultRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(false);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(),
            VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            RepairAlgorithm.LATEST);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>MAJORITY</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * Cause there are no missing keys: neither totally missing, nor available only in deferred delete queue, MAJORITY
     * algorithm won't be used, LATEST will be used instead.
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * LATEST in given case algorithm and also contains the value that was used for fix (val3 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes  has same value that is equal to val3.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMissedUpdateOnPrimaryWithMajorityRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(false);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "MAJORITY", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(),
            VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            RepairAlgorithm.LATEST);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>PRIMARY</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * Cause there are no missing keys neither totally missing, nor available only in deferred delete queue, PRIMARY
     * algorithm won't be used, LATEST will be used instead.
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * LATEST in given case algorithm and also contains the value that was used for fix (val3 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes  has same value that is equal to val3.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMissedUpdateOnPrimaryWithPrimaryRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(false);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "PRIMARY", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(),
            VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            RepairAlgorithm.LATEST);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>LATEST</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * LATEST in given case algorithm and also contains the value that was used for fix (val3 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes  has same value that is equal to val3.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMissedUpdateOnPrimaryWithMaxGridCacheVersionRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(false);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "LATEST", "--recheck-delay", "0", "--local-output"));

        // Validate partition reconciliation result and enusre that invalid key was successfully fixed.
        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(),
            VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            RepairAlgorithm.LATEST);
    }

    /**
     * <b>Prerequisites:</b>
     * Start cluster with 4 nodes and create <b>atomic/transactional non-persistent/persistent</b> cache with 3 backups.
     * Populate cache with some data.
     * <p>
     * <b>Steps:</b>
     * <or>
     * <li>Add one more entry with inconsistent data:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Run partition_reconciliation command in fix mode with fix-alg <b>PRINT_ONLY</b>.</li>
     * <p>
     * <b>Expected result:</b>
     * Cause there are no missing keys neither totally missing, nor available only in deferred delete queue, PRINT_ONLY
     * algorithm won't be used, LATEST will be used instead.
     * <ul>
     * <li>Parse output and ensure that one key was added to 'inconsistent keys section' with corresponding
     * versions and values:
     * <ul>
     * <li>Primary:  key has ver0  and val0</li>
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     * </li>
     * <li>Also ensure that key has repair meta info, that reflects that keys were fixed using expected algorithm
     * LATEST in given case algorithm and also contains the value that was used for fix (val3 in given case).</li>
     * <li>Ensure that previously inconsistent key on all 4 nodes  has same value that is equal to val3.</li>
     * </ul>
     * </or>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMissedUpdateOnPrimaryWithPrintOnlyRepairAlg() throws Exception {
        PartitionReconciliationDataRowMeta invalidDataRowMeta = populateCacheWithInconsistentEntry(false);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--cache", "partition_reconciliation", "--repair",
            "--recheck-delay", "0", "--local-output"));

        validateFix(invalidDataRowMeta, hnd.getLastOperationResult(),
            VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2,
            RepairAlgorithm.LATEST);

    }

    /**
     * Validate partition reconciliation result and Enusre that invalid key was successfully fixed.
     *
     * @param invalidDataRowMeta Invalidated data row.
     * @param res Partition reconciliation result.
     * @param invalidVal Invalid value.
     * @param repairAlg Used repair algorithm.
     * @throws IgniteCheckedException If failed to marshal expected value within repair meta.
     */
    private void validateFix(
        PartitionReconciliationDataRowMeta invalidDataRowMeta,
        ReconciliationResult res,
        String invalidVal,
        RepairAlgorithm repairAlg
    ) throws IgniteCheckedException {
        // Validate partition reconciliation result.
        validateResult(
            invalidDataRowMeta,
            res,
            new PartitionReconciliationRepairMeta(
                true,
                invalidVal != null ? new PartitionReconciliationValueMeta(
                    grid(0).context().cacheObjects().marshal(
                        ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().cacheObjectContext(),
                        invalidVal),
                    invalidVal,
                    null) : null,
                repairAlg));

        for (int i = 0; i < 4; i++) {
            Object val = ignite(i).cachex(DEFAULT_CACHE_NAME).localPeek(INVALID_KEY, null, null);

            assertEquals(invalidVal, val);
        }
    }

    /**
     * Prepare inconsistent entry with following values and versions:
     * <ul>
     * {@code if(dropEntry)}
     * <li>Primary:  key is missing</li>
     * else
     * <li>Primary:  key has ver0  and val0</li>
     * In all cases:
     * <li>Backup 1: key has ver1 > ver1  and val1</li>
     * <li>Backup 2: key has ver1 > ver1  and val1 (same as backup 1)</li>
     * <li>Backup 3: key has ver2 > ver1  and val3</li>
     * </ul>
     *
     * @param dropEntry Boolean flag in order to define whether to drop entry on primary or not.
     * @return List of affinity nodes.
     */
    private PartitionReconciliationDataRowMeta populateCacheWithInconsistentEntry(boolean dropEntry)
        throws IgniteCheckedException {
        Map<UUID, PartitionReconciliationValueMeta> valMetas = new HashMap<>();

        GridCacheContext<Object, Object> cctx = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context();

        PartitionReconciliationDataRowMeta res = new PartitionReconciliationDataRowMeta(
            new PartitionReconciliationKeyMeta(
                grid(0).context().cacheObjects().marshal(cctx.cacheObjectContext(), INVALID_KEY),
                String.valueOf(INVALID_KEY)),
            valMetas);

        List<ClusterNode> nodes = cctx.affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        if (dropEntry) {
            ((IgniteEx)grid(nodes.get(0))).cachex(DEFAULT_CACHE_NAME).
                clearLocallyAll(Collections.singleton(INVALID_KEY), true, true, true);
        }
        else {
            GridCacheVersion invalidVerNode0 = new GridCacheVersion(0, 0, 1);

            corruptDataEntry(((IgniteEx)grid(nodes.get(0))).cachex(
                DEFAULT_CACHE_NAME).context(),
                INVALID_KEY,
                false,
                true,
                invalidVerNode0,
                BROKEN_POSTFIX_1);

            String brokenValNode0 = VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_1;

            valMetas.put(
                nodes.get(0).id(),
                new PartitionReconciliationValueMeta(
                    grid(0).context().cacheObjects().marshal(cctx.cacheObjectContext(), brokenValNode0),
                    brokenValNode0,
                    invalidVerNode0));
        }

        //Node 1
        GridCacheVersion invalidVerNode1 = new GridCacheVersion(0, 0, 1);

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            invalidVerNode1,
            null);

        String brokenValNode1 = VALUE_PREFIX + INVALID_KEY;

        valMetas.put(
            nodes.get(1).id(),
            new PartitionReconciliationValueMeta(
                grid(1).context().cacheObjects().marshal(cctx.cacheObjectContext(), brokenValNode1),
                brokenValNode1,
                invalidVerNode1));

        //Node 2
        GridCacheVersion invalidVerNode2 = new GridCacheVersion(0, 0, 2);

        corruptDataEntry(((IgniteEx)grid(nodes.get(2))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            invalidVerNode2,
            null);

        String brokenValNode2 = VALUE_PREFIX + INVALID_KEY;

        valMetas.put(
            nodes.get(2).id(),
            new PartitionReconciliationValueMeta(
                grid(2).context().cacheObjects().marshal(cctx.cacheObjectContext(), brokenValNode2),
                brokenValNode2,
                invalidVerNode2));

        // Node 3
        GridCacheVersion invalidVerNode3 = new GridCacheVersion(0, 0, 3);

        corruptDataEntry(((IgniteEx)grid(nodes.get(3))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            true,
            invalidVerNode3,
            BROKEN_POSTFIX_2);

        String brokenValNode3 = VALUE_PREFIX + INVALID_KEY + BROKEN_POSTFIX_2;

        valMetas.put(
            nodes.get(3).id(),
            new PartitionReconciliationValueMeta(
                grid(3).context().cacheObjects().marshal(cctx.cacheObjectContext(), brokenValNode3),
                brokenValNode3,
                invalidVerNode3));

        return res;
    }

    /**
     * Utility method to validate partition reconciliation result.
     *
     * @param invalidDataRowMeta Invalidated data row.
     * @param res Partition reconciliation result.
     * @param repairMeta Repair meta.
     */
    private void validateResult(
        PartitionReconciliationDataRowMeta invalidDataRowMeta,
        ReconciliationResult res,
        PartitionReconciliationRepairMeta repairMeta) {
        assertEquals(0, res.errors().size());
        assertEquals(0, res.partitionReconciliationResult().skippedEntriesCount());
        assertEquals(0, res.partitionReconciliationResult().skippedCachesCount());
        assertEquals(1, res.partitionReconciliationResult().inconsistentKeysCount());

        assertEquals(
            Collections.singletonMap(
                DEFAULT_CACHE_NAME,
                Collections.singletonMap(
                    ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().partition(INVALID_KEY),
                    Collections.singletonList(
                        new PartitionReconciliationDataRowMeta(
                            invalidDataRowMeta.keyMeta(),
                            invalidDataRowMeta.valueMeta(),
                            repairMeta)))),
            res.partitionReconciliationResult().inconsistentKeys());
    }
}
