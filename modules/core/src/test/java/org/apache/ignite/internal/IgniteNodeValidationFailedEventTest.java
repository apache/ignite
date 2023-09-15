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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.NodeValidationFailedEvent;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_VALIDATION_FAILED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests joining node validation failed event. */
public class IgniteNodeValidationFailedEventTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setIncludeEventTypes(EVT_NODE_VALIDATION_FAILED)
            .setConsistentId(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testNodeValidationFailedEvent() throws Exception {
        startGrid(0);

        CountDownLatch evtLatch = new CountDownLatch(1);

        AtomicReference<Event> listenedEvtRef = new AtomicReference<>();

        grid(0).events().localListen(evt -> {
            assertTrue(listenedEvtRef.compareAndSet(null, evt));

            evtLatch.countDown();

            return true;
        }, EVT_NODE_VALIDATION_FAILED);

        startGrid(1);

        String invalidNodeName = getTestIgniteInstanceName(2);

        IgniteConfiguration invalidCfg = getConfiguration(invalidNodeName)
            .setPluginProviders(new TestSecurityPluginProvider("login", "", ALL_PERMISSIONS, false));

        assertThrowsWithCause(() -> startGrid(optimize(invalidCfg)), IgniteSpiException.class);

        evtLatch.await();

        Event listenedEvt = listenedEvtRef.get();

        assertTrue(listenedEvt instanceof NodeValidationFailedEvent);

        NodeValidationFailedEvent validationEvt = (NodeValidationFailedEvent)listenedEvt;

        assertEquals(invalidNodeName, validationEvt.eventNode().attribute(ATTR_NODE_CONSISTENT_ID));

        IgniteNodeValidationResult validationRes = validationEvt.validationResult();

        assertNotNull(validationRes);

        String errMsg = validationRes.message();

        assertNotNull(errMsg);
        assertTrue(errMsg.contains(
            "Local node's grid security processor class is not equal to remote node's grid security processor class"));
    }

    /** */
    @Test
    public void testEventDisabledByDefault() throws Exception {
        IgniteEx ignite = startGrid(super.getConfiguration(getTestIgniteInstanceName(0)));

        assertFalse(ignite.context().event().isRecordable(EVT_NODE_VALIDATION_FAILED));
    }
}
