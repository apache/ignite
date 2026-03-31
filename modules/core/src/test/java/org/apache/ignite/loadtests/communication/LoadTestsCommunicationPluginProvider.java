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

package org.apache.ignite.loadtests.communication;

import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;

/** Registers communication messages used by load tests. */
public class LoadTestsCommunicationPluginProvider extends AbstractTestPluginProvider {
    /** {@inheritDoc} */
    @Override public String name() {
        return "LOAD_TESTS_COMMUNICATION_PLUGIN";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        registry.registerExtension(
            MessageFactoryProvider.class,
            f -> f.register(10_000, GridTestMessage::new, new GridTestMessageSerializer())
        );
    }
}
