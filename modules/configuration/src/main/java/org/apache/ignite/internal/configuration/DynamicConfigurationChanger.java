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

package org.apache.ignite.internal.configuration;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/**
 * Interface to provide configuration access to up-to-date configuration trees in {@link DynamicConfiguration},
 * {@link NamedListConfiguration} and {@link DynamicProperty}.
 */
public interface DynamicConfigurationChanger {
    /**
     * Changes the configuration.
     *
     * @param source Configuration source to create patch from.
     * @return Future that is completed on change completion.
     */
    CompletableFuture<Void> change(ConfigurationSource source);

    /**
     * Get root node by root key.
     *
     * @param rootKey Root key.
     * @return Root node.
     */
    InnerNode getRootNode(RootKey<?, ?> rootKey);
}
