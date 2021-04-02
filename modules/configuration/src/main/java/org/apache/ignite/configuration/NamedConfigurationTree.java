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

package org.apache.ignite.configuration;

import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.tree.NamedListChange;
import org.apache.ignite.configuration.tree.NamedListView;

/**
 * Configuration tree representing arbitrary set of named underlying configuration tree of the same type.
 *
 * @param <T> Type of the underlying configuration tree.
 * @param <VIEW> Value type of the underlying node.
 * @param <CHANGE> Type of the object that changes underlying nodes values.
 */
public interface NamedConfigurationTree<T extends ConfigurationProperty<VIEW, CHANGE>, VIEW, CHANGE, INIT>
    extends ConfigurationTree<NamedListView<VIEW>, NamedListChange<CHANGE, INIT>>
{
    /**
     * Get named configuration by name.
     * @param name Name.
     * @return Configuration.
     */
    T get(String name);

    /**
     * Add named-list-specific configuration values listener.
     * @param listener Listener.
     */
    void listen(ConfigurationNamedListListener<VIEW> listener);
}
