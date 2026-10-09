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

package org.apache.ignite.internal.metric;

import java.util.stream.StreamSupport;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.spi.systemview.view.PluginView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor.PLUGINS_SYS_VIEW;

/** Tests for {@link SystemView} for plugins. */
public class SystemViewPluginTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testPluginView() throws Exception {
        try (IgniteEx n = startGrid(getConfiguration().setPluginProviders(new TestPluginProvider()))) {
            SystemView<PluginView> view = n.context().systemView().view(PLUGINS_SYS_VIEW);

            PluginView testPluginView = StreamSupport.stream(view.spliterator(), false)
                .filter(v -> "FOR_SYS_VIEW_PLUGIN_NAME".equals(v.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Plugin not found"));

            assertEquals("FOR_SYS_VIEW_PLUGIN_NAME", testPluginView.name());
            assertEquals("FOR_SYS_VIEW_PLUGIN_INFO", testPluginView.info());
            assertEquals("42", testPluginView.version());
            assertEquals(TestPluginProvider.TestPlugin.class.getName(), testPluginView.className());
        }
    }

    /** */
    private static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "FOR_SYS_VIEW_PLUGIN_NAME";
        }

        /** {@inheritDoc} */
        @Override public String version() {
            return "42";
        }

        /** {@inheritDoc} */
        @Override public String info() {
            return "FOR_SYS_VIEW_PLUGIN_INFO";
        }

        /** {@inheritDoc} */
        @Override public <T extends IgnitePlugin> T plugin() {
            return (T)new TestPlugin();
        }

        /** */
        private static class TestPlugin implements IgnitePlugin {
        }
    }
}
