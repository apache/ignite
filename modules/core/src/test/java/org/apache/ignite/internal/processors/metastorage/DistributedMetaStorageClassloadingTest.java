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

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test for {@link DistributedMetaStorageImpl} issues with classloading.
 */
public class DistributedMetaStorageClassloadingTest extends GridCommonAbstractTest {
    /** Failure handler that keeps count of failures (initialized before every test). */
    private CountingFailureHandler failureHandler;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.isClientMode())
            cfg.setClassLoader(getExternalClassLoader());

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return failureHandler;
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();
        failureHandler = new CountingFailureHandler();
    }

    /** Test that receiving data of unknown class into distributed metastorage doesn't lead to failure. */
    @Test
    public void testWontFailReceivingDataOfUnknownClass() throws Exception {
        startGrid(1);

        IgniteEx client = startClientGrid(0);

        client.context().distributedMetastorage().write("hey", serverUnknown("one"));
        client.context().distributedMetastorage().write("hey", serverUnknown("two"));

        assertEquals(0, failureHandler.getCount());
    }

    /** Test that reading data of unknown class from distributed metastorage doesn't lead to failure. */
    @Test
    public void testWontFailReadingDataOfUnknownClass() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteEx client = startClientGrid(0);

        client.context().distributedMetastorage().write("hey", serverUnknown("0"));

        assertThrowsWithCause(() -> ignite.context().distributedMetastorage().read("hey"), ClassNotFoundException.class);

        assertEquals(0, failureHandler.getCount());
    }


    /**
     * Test that listening for data of unknown class from distributed metastorage lead to failure.
     *
     * Description:
     * Start server node with exclusion of certain BamboozleClass (this is done via system property
     * which adds class filter to class loader).
     * Add listener to a server's distributed metadata.
     * Start client node and write new instance of BamboozleClass to the distributed metastorage.
     * This should lead to a failure on a server node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailListeningForDataOfUnknownClass() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteEx client = startClientGrid(0);

        ignite.context().distributedMetastorage().listen("hey"::equals, (key, oldVal, newVal) -> System.out.println(newVal));

        client.context().distributedMetastorage().write("hey", serverUnknown("0"));

        assertEquals(1, failureHandler.getCount());
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    /**
     * Failure handler that only keeps count of failures.
     */
    private static class CountingFailureHandler implements FailureHandler {

        /**
         * Count of failures.
         * */
        private int count = 0;

        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            count++;
            return false;
        }

        /**
         * Get count of failures.
         */
        public int getCount() {
            return count;
        }
    }

    /** */
    private static Serializable serverUnknown(String name) throws Exception {
        return (Serializable)getExternalClassLoader().loadClass("org.apache.ignite.tests.p2p.cache.Person")
            .getConstructor(String.class).newInstance(name);
    }

}
