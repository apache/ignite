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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Checks that a cache with an empty cache group name is rejected.
 */
public class CacheGroupNameValidationTest extends GridCommonAbstractTest {
    /** Expected error message. */
    private static final String ERR_MSG = "Cache group name must not be empty.";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateCache() throws Exception {
        IgniteEx srv = startGrid(0);

        assertThrowsAnyCause(log, () -> srv.createCache(new CacheConfiguration<>("cache1").setGroupName("")),
            IllegalArgumentException.class, ERR_MSG);
    }

    /** @throws Exception If failed. */
    @Test
    public void testThinClientCreateCache() throws Exception {
        startGrid(0);

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            Throwable e = assertThrowsWithCause(
                () -> cli.createCache(new ClientCacheConfiguration().setName("cache1").setGroupName("")),
                ClientException.class);

            assertTrue(e.getMessage(), e.getMessage().contains(ERR_MSG));
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testStaticConfiguration() throws Exception {
        startGrid(0);

        assertThrowsAnyCause(log, () -> startGrid(getConfiguration(getTestIgniteInstanceName(1))
            .setCacheConfiguration(new CacheConfiguration<>("cache1").setGroupName(""))),
            IllegalArgumentException.class, ERR_MSG);
    }
}
