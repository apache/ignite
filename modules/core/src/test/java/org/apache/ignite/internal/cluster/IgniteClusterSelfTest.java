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

package org.apache.ignite.internal.cluster;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_STOP;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_STOP;

/**
 * {@link IgniteClusterImpl} implementation tests.
 */
public class IgniteClusterSelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testLifecycle() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        LifecycleBeanTest lifecycleBean = new LifecycleBeanTest();

        // Provide lifecycle bean to configuration.
        cfg.setLifecycleBeans(lifecycleBean);

        try (Ignite ignite  = Ignition.start(cfg)) {
            // No-op.
        }

        assertTrue(lifecycleBean.eventQueue.size() == 4);
        assertTrue(lifecycleBean.eventQueue.poll() == BEFORE_NODE_START);
        assertTrue(lifecycleBean.eventQueue.poll() == AFTER_NODE_START);
        assertTrue(lifecycleBean.eventQueue.poll() == BEFORE_NODE_STOP);
        assertTrue(lifecycleBean.eventQueue.poll() == AFTER_NODE_STOP);
    }

    /**
     * Simple {@link LifecycleBean} implementation.
     */
    public static class LifecycleBeanTest implements LifecycleBean {
        /** Auto-inject ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Event queue. */
        public ConcurrentLinkedQueue<LifecycleEventType> eventQueue = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            eventQueue.add(evt);

            // check nodeLocalMap is not locked
            ConcurrentMap map = ignite.cluster().nodeLocalMap();

            assertNotNull(map);
        }
    }

}
