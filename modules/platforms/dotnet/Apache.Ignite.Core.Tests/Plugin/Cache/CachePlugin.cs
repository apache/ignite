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

namespace Apache.Ignite.Core.Tests.Plugin.Cache
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Plugin.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Test cache plugin.
    /// </summary>
    public class CachePlugin : ICachePluginProvider<CachePluginConfiguration>
    {
        /** */
        private static readonly ConcurrentDictionary<CachePlugin, object> Instances = 
            new ConcurrentDictionary<CachePlugin, object>();

        /// <summary>
        /// Gets the instances.
        /// </summary>
        public static IEnumerable<CachePlugin> GetInstances()
        {
            return Instances.Keys;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePlugin"/> class.
        /// </summary>
        public CachePlugin()
        {
            Assert.IsTrue(Instances.TryAdd(this, null));
        }

        /** <inheritdoc /> */
        public void Start(ICachePluginContext<CachePluginConfiguration> cachePluginContext)
        {
            Context = cachePluginContext;
            Started = true;

            Assert.IsNotNull(Context);
            Assert.IsNotNull(Context.IgniteConfiguration);
            Assert.IsNotNull(Context.CachePluginConfiguration);
            Assert.IsNotNull(Context.CacheConfiguration);
            Assert.IsNotNull(Context.Ignite);

            // Check that Ignite is operational.
            Assert.GreaterOrEqual(2, Context.Ignite.GetCluster().GetNodes().Count);

            Throw();
        }

        /** <inheritdoc /> */
        public void Stop(bool cancel)
        {
            Stopped = cancel;

            object unused;
            Assert.IsTrue(Instances.TryRemove(this, out unused));
        }

        /** <inheritdoc /> */
        public void OnIgniteStart()
        {
            // Check that Ignite is operational.
            Assert.GreaterOrEqual(2, Context.Ignite.GetCluster().GetNodes().Count);

            IgniteStarted = true;
        }

        /** <inheritdoc /> */
        public void OnIgniteStop(bool cancel)
        {
            IgniteStopped = cancel;
        }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="CachePlugin"/> is started.
        /// </summary>
        public bool Started { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="CachePlugin"/> is started.
        /// </summary>
        public bool IgniteStarted { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="CachePlugin"/> is stopped.
        /// </summary>
        public bool? Stopped { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="CachePlugin"/> is stopped.
        /// </summary>
        public bool? IgniteStopped { get; private set; }

        /// <summary>
        /// Gets the context.
        /// </summary>
        public ICachePluginContext<CachePluginConfiguration> Context { get; private set; }

        /// <summary>
        /// Throws an error when configured.
        /// </summary>
        private void Throw()
        {
            if (Context.CachePluginConfiguration.ThrowError)
                throw new IOException("Failure in cache plugin provider");
        }
    }
}