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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class IgniteLifecycle {

    @Test
    void startNode() {
        //tag::start[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        Ignite ignite = Ignition.start(cfg);
        //end::start[]
        ignite.close();
    }

    @Test
    void startAndClose() {

        //tag::autoclose[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        try (Ignite ignite = Ignition.start(cfg)) {
            //
        }

        //end::autoclose[]
    }

    void startClientNode() {
        //tag::client-node[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Enable client mode.
        cfg.setClientMode(true);

        // Start a client 
        Ignite ignite = Ignition.start(cfg);
        //end::client-node[]

        ignite.close();
    }

    @Test
    void lifecycleEvents() {
        //tag::lifecycle[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Specify a lifecycle bean in the node configuration.
        cfg.setLifecycleBeans(new MyLifecycleBean());

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::lifecycle[]

        ignite.close();
    }
}
