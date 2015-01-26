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

package org.apache.ignite.examples.misc.lifecycle;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;

import static org.apache.ignite.lifecycle.LifecycleEventType.*;

/**
 * This example shows how to provide your own {@link org.apache.ignite.lifecycle.LifecycleBean} implementation
 * to be able to hook into GridGain lifecycle. The {@link LifecycleExampleBean} bean
 * will output occurred lifecycle events to the console.
 * <p>
 * This example does not require remote nodes to be started.
 */
public final class LifecycleExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        System.out.println();
        System.out.println(">>> Lifecycle example started.");

        // Create new configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        LifecycleExampleBean bean = new LifecycleExampleBean();

        // Provide lifecycle bean to configuration.
        cfg.setLifecycleBeans(bean);

        try (Ignite g  = Ignition.start(cfg)) {
            // Make sure that lifecycle bean was notified about grid startup.
            assert bean.isStarted();
        }

        // Make sure that lifecycle bean was notified about grid stop.
        assert !bean.isStarted();
    }

    /**
     * Simple {@link org.apache.ignite.lifecycle.LifecycleBean} implementation that outputs event type when it is occurred.
     */
    public static class LifecycleExampleBean implements LifecycleBean {
        /** Auto-inject grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Started flag. */
        private boolean isStarted;

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            System.out.println();
            System.out.println(">>> Grid lifecycle event occurred: " + evt);
            System.out.println(">>> Grid name: " + ignite.name());

            if (evt == AFTER_GRID_START)
                isStarted = true;
            else if (evt == AFTER_GRID_STOP)
                isStarted = false;
        }

        /**
         * @return {@code True} if grid has been started.
         */
        public boolean isStarted() {
            return isStarted;
        }
    }
}
