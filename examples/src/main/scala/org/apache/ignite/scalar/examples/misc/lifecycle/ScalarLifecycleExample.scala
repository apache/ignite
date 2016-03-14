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

package org.apache.ignite.scalar.examples.misc.lifecycle

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.examples.misc.lifecycle.LifecycleExample.LifecycleExampleBean
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lifecycle.LifecycleEventType._
import org.apache.ignite.lifecycle.{LifecycleBean, LifecycleEventType}
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.{Ignite, Ignition}

/**
 * This example shows how to provide your own [[LifecycleBean]] implementation
 * to be able to hook into Ignite lifecycle. The [[LifecycleExampleBean]] bean
 * will output occurred lifecycle events to the console.
 * <p>
 * This example does not require remote nodes to be started.
 */
object ScalarLifecycleExample extends App {
    println()
    println(">>> Lifecycle example started.")

    // Create new configuration.
    val cfg = new IgniteConfiguration

    val bean = new LifecycleExampleBean

    // Provide lifecycle bean to configuration.
    cfg.setLifecycleBeans(bean)

    val ignite = Ignition.start(cfg)

    try {
        assert(bean.isStarted)
    }
    finally {
        if (ignite != null)
            ignite.close()
    }

    // Make sure that lifecycle bean was notified about ignite stop.
    assert(!bean.isStarted)

    /**
     * Simple [[LifecycleBean]] implementation that outputs event type when it is occurred.
     */
    class LifecycleExampleBean extends LifecycleBean {
        /** Auto-inject ignite instance. */
        @IgniteInstanceResource private var ignite: Ignite = null
        
        /** Started flag. */
        private var isStartedInt = false

        @impl def onLifecycleEvent(evt: LifecycleEventType) {
            println()
            println(">>> Lifecycle event occurred: " + evt)
            println(">>> Ignite name: " + ignite.name)
            
            if (evt eq AFTER_NODE_START) 
                isStartedInt = true
            else if (evt eq AFTER_NODE_STOP) 
                isStartedInt = false
        }

        /**
         * @return `True` if ignite has been started.
         */
        def isStarted: Boolean = {
            isStartedInt
        }
    }
}
