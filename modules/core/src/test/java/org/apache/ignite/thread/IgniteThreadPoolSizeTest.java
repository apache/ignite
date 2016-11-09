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

package org.apache.ignite.thread;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for wrong values of xxxThreadPoolSize
 */
public class IgniteThreadPoolSizeTest extends GridCommonAbstractTest {

    /** Wrong thread pool size value for testing */
    private static final int WRONG_VALUE = 0;

    /** Factory method for creating an IgniteConfiguration */
    private IgniteConfiguration cfg() {
        return new IgniteConfiguration();
    }

    /** Performs testing for wrong tread pool size
     * @param cfg an IgniteConfiguration with the only one thread pool size assigned with the WRONG_VALUE
     * @throws Exception If starting the Ignition doesn't throw an exception related to the 'thread pool size'
     */
    private void testWrongPoolSize(IgniteConfiguration cfg) throws Exception {
        try {
            Ignition.start(cfg);
            throw new Exception("test failed");
        }
        catch (IgniteException ex) {
            if (ex.getMessage() == null ||
                !ex.getMessage().contains("thread pool size"))
                throw ex;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncCallbackPoolSize() throws Exception {
        testWrongPoolSize(cfg().setAsyncCallbackPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgfsThreadPoolSize() throws Exception {
        testWrongPoolSize(cfg().setIgfsThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testManagementThreadPoolSize() throws Exception {
        testWrongPoolSize(cfg().setManagementThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeerClassLoadingThreadPoolSize() throws Exception {
        testWrongPoolSize(cfg().setPeerClassLoadingThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPublicThreadPoolSize() throws Exception {
        testWrongPoolSize(cfg().setPublicThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceThreadPoolSize() throws Exception {
        testWrongPoolSize(cfg().setRebalanceThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSystemThreadPoolSize() throws Exception {
        testWrongPoolSize(cfg().setSystemThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUtilityCachePoolSize() throws Exception {
        testWrongPoolSize(cfg().setUtilityCachePoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecated")
    public void testMarshallerCachePoolSize() throws Exception {
        testWrongPoolSize(cfg().setMarshallerCachePoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectorThreadPoolSize() throws Exception {
        final IgniteConfiguration cfg = cfg();
        cfg.getConnectorConfiguration().setThreadPoolSize(WRONG_VALUE);
        testWrongPoolSize(cfg);
    }
}
