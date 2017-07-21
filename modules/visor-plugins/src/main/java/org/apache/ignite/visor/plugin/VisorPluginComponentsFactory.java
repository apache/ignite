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

package org.apache.ignite.visor.plugin;

import java.util.List;
import javax.swing.JPanel;

/**
 * Factory for creating ready Visor UI blocks like nodes panel, log panel, etc.
 * Plugins will receive factory instance from
 * {@link VisorPluggableTab#createPanel(VisorPluginComponentsFactory, JPanel)} method.
 */
public interface VisorPluginComponentsFactory {
    /**
     * Create panel with nodes.
     *
     * @param pluginName Plugin name.
     * @param title Panel title.
     * @param ovrMsg Overlay message text.
     * @param showHostBtn Whether or not host button should be displayed.
     * @return Nodes panel.
     */
    public VisorPluginNodesPanel nodesPanel(String pluginName, String title, List<String> ovrMsg, boolean showHostBtn);

    /**
     * Create panel with list of log events.
     *
     * @param pluginName Plugin name.
     * @param title Panel title.
     * @param ovrMsg Overlay message text.
     * @return Log panel.
     */
    public VisorPluginLogPanel logPanel(String pluginName, String title, List<String> ovrMsg);
}