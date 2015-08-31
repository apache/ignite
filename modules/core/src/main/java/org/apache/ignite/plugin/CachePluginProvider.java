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

package org.apache.ignite.plugin;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Cache plugin provider is a point for processing of properties 
 * which provide specific {@link CachePluginConfiguration}.
 */
public interface CachePluginProvider<C extends CachePluginConfiguration> {    
    /**
     * Starts grid component.
     *
     * @throws IgniteCheckedException Throws in case of any errors.
     */
    public void start() throws IgniteCheckedException;

    /**
     * Stops grid component.
     *
     * @param cancel If {@code true}, then all ongoing tasks or jobs for relevant
     *      components need to be cancelled.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void stop(boolean cancel);

    /**
     * Callback that notifies that Ignite has successfully started,
     * including all internal components.
     *
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void onIgniteStart() throws IgniteCheckedException;

    /**
     * Callback to notify that Ignite is about to stop.
     *
     * @param cancel Flag indicating whether jobs should be canceled.
     */
    public void onIgniteStop(boolean cancel);

    /**
     * @param cls Ignite component class.
     * @return Ignite component or {@code null} if component is not supported.
     */
    @Nullable public <T> T createComponent(Class<T> cls);

    /**
     * Validates cache plugin configuration in process of cache creation. Throw exception if validation failed.
     *
     * @throws IgniteCheckedException If validation failed.
     */
    public void validate() throws IgniteCheckedException;

    /**
     * Checks that remote caches has configuration compatible with the local.
     *
     * @param locCfg Local configuration.
     * @param locPluginCcfg Local plugin configuration.
     * @param rmtCfg Remote configuration.
     * @param rmtNode Remote node.
     */
    public void validateRemote(CacheConfiguration locCfg, C locPluginCcfg, CacheConfiguration rmtCfg, ClusterNode rmtNode)
        throws IgniteCheckedException;
}