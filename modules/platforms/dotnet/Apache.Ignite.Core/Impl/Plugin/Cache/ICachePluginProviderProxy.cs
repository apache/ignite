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

namespace Apache.Ignite.Core.Impl.Plugin.Cache
{
    using System;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Plugin.Cache;

    /// <summary>
    /// Wraps user-defined generic <see cref="ICachePluginProvider{TConfig}"/>.
    /// </summary>
    internal interface ICachePluginProviderProxy
    {
        /// <summary>
        /// Starts the plugin provider.
        /// </summary>
        void Start(IgniteConfiguration igniteConfiguration, CacheConfiguration cacheConfiguration, IIgnite ignite);

        /// <summary>
        /// Stops the plugin provider.
        /// </summary>
        /// <param name="cancel">if set to <c>true</c>, all ongoing operations should be canceled.</param>
        void Stop(bool cancel);

        /// <summary>
        /// Called when Ignite has been started and is fully functional.
        /// <para />
        /// Use <see cref="IIgnite.Stopping"/> and <see cref="IIgnite.Stopped"/> to track shutdown process.
        /// </summary>
        void OnIgniteStart();

        /// <summary>
        /// Callback to notify that Ignite is about to stop.
        /// </summary>
        /// <param name="cancel">if set to <c>true</c>, all ongoing operations should be canceled.</param>
        void OnIgniteStop(bool cancel);
    }
}
