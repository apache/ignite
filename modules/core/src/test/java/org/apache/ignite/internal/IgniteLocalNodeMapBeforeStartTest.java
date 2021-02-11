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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_STOP;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_STOP;

/**
 *
 */
public class IgniteLocalNodeMapBeforeStartTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLocalMapFromLifecycleBean() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        LifecycleBeanTest lifecycleBean = new LifecycleBeanTest();

        // Provide lifecycle bean to configuration.
        cfg.setLifecycleBeans(lifecycleBean);

        try (Ignite ignite = Ignition.start(cfg)) {
            // No-op.
        }

        assertTrue(lifecycleBean.evtQueue.size() == 4);
        assertTrue(lifecycleBean.evtQueue.poll() == BEFORE_NODE_START);
        assertTrue(lifecycleBean.evtQueue.poll() == AFTER_NODE_START);
        assertTrue(lifecycleBean.evtQueue.poll() == BEFORE_NODE_STOP);
        assertTrue(lifecycleBean.evtQueue.poll() == AFTER_NODE_STOP);
    }

    /**
     * Simple {@link LifecycleBean} implementation.
     */
    private static class LifecycleBeanTest implements LifecycleBean {
        /** Auto-inject ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Event queue. */
        ConcurrentLinkedQueue<LifecycleEventType> evtQueue = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            evtQueue.add(evt);

            // check nodeLocalMap is not locked
            ConcurrentMap map = ignite.cluster().nodeLocalMap();

            assertNotNull(map);
        }
    }
}
