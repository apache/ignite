/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.thread;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteThreadPoolSizeTest extends GridCommonAbstractTest {
    /** Wrong thread pool size value for testing */
    private static final int WRONG_VALUE = 0;

    /**
     * @return Ignite configuration.
     */
    private IgniteConfiguration configuration() {
        return new IgniteConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAsyncCallbackPoolSize() throws Exception {
        testWrongPoolSize(configuration().setAsyncCallbackPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgfsThreadPoolSize() throws Exception {
        testWrongPoolSize(configuration().setIgfsThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testManagementThreadPoolSize() throws Exception {
        testWrongPoolSize(configuration().setManagementThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeerClassLoadingThreadPoolSize() throws Exception {
        testWrongPoolSize(configuration().setPeerClassLoadingThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPublicThreadPoolSize() throws Exception {
        testWrongPoolSize(configuration().setPublicThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceThreadPoolSize() throws Exception {
        testWrongPoolSize(configuration().setRebalanceThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemThreadPoolSize() throws Exception {
        testWrongPoolSize(configuration().setSystemThreadPoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUtilityCachePoolSize() throws Exception {
        testWrongPoolSize(configuration().setUtilityCachePoolSize(WRONG_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectorThreadPoolSize() throws Exception {
        final IgniteConfiguration cfg = configuration();

        cfg.getConnectorConfiguration().setThreadPoolSize(WRONG_VALUE);

        testWrongPoolSize(cfg);
    }

    /**
     * Performs testing for wrong tread pool size.
     *
     * @param cfg an IgniteConfiguration with the only one thread pool size assigned with the WRONG_VALUE.
     * @throws Exception If failed.
     */
    private void testWrongPoolSize(IgniteConfiguration cfg) throws Exception {
        try {
            Ignition.start(cfg);

            fail();
        }
        catch (IgniteException ex) {
            assertNotNull(ex.getMessage());
            assertTrue(ex.getMessage().contains("thread pool size"));
        }
    }
}
