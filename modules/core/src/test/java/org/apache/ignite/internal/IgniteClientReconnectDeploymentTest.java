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

package org.apache.ignite.internal;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY;

/**
 * Test local class deployment on client reconnect.
 */
@WithSystemProperty(key = IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY, value = "1000")
public class IgniteClientReconnectDeploymentTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPeerClassLoadingEnabled(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployDuringReconnect() throws Exception {
        IgniteEx client = grid(serverCount());

        Ignite srv = ignite(0);

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache("test_cache");

        reconnectClientNode(client, srv, () -> {
            try {
                client.context().deploy().deploy(TestEntryProcessor.class, TestEntryProcessor.class.getClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new AssertionError(e);
            }
        });

        assertTrue(cache.invoke(0, new TestEntryProcessor()));
    }

    /** */
    private static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Integer, Integer> entry, Object... args) {
            return true;
        }
    }
}
