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

import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.tree.InnerNode;

/** */
public abstract class RootKey<T extends ConfigurationTree<VIEW, ?>, VIEW> {
    /**
     * @return Name of the configuration root.
     */
    public abstract String key();

    /**
     * @return Configuration type of the root.
     */
    protected abstract ConfigurationType type();

    /**
     * @return New instance of the inner node for the root.
     */
    protected abstract InnerNode createRootNode();

    /**
     * @param changer Configuration changer instance.
     * @return New instance of the public tree for the root.
     */
    protected abstract T createPublicRoot(ConfigurationChanger changer);
}
