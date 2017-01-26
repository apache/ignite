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

namespace Apache.Ignite.ExamplesDll.Services
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service implementation.
    /// </summary>
    [Serializable]
    public class MapService<TK, TV> : IService
    {
        /** Injected Ignite instance. */
        [InstanceResource] private readonly IIgnite _ignite;

        /** Cache. */
        private ICache<TK, TV> _cache;

        /// <summary>
        /// Initializes this instance before execution.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        public void Init(IServiceContext context)
        {
            // Create a new cache for every service deployment.
            // Note that we use service name as cache name, which allows
            // for each service deployment to use its own isolated cache.
            _cache = _ignite.GetOrCreateCache<TK, TV>("MapService_" + context.Name);

            Console.WriteLine("Service initialized: " + context.Name);
        }

        /// <summary>
        /// Starts execution of this service. This method is automatically invoked whenever an instance of the service
        /// is deployed on an Ignite node. Note that service is considered deployed even after it exits the Execute
        /// method and can be cancelled (or undeployed) only by calling any of the Cancel methods on 
        /// <see cref="IServices"/> API. Also note that service is not required to exit from Execute method until
        /// Cancel method was called.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        public void Execute(IServiceContext context)
        {
            Console.WriteLine("Service started: " + context.Name);
        }

        /// <summary>
        /// Cancels this instance.
        /// <para/>
        /// Note that Ignite cannot guarantee that the service exits from <see cref="IService.Execute"/>
        /// method whenever <see cref="IService.Cancel"/> is called. It is up to the user to
        /// make sure that the service code properly reacts to cancellations.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        public void Cancel(IServiceContext context)
        {
            Console.WriteLine("Service cancelled: " + context.Name);
        }

        /// <summary>
        /// Puts an entry to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void Put(TK key, TV value)
        {
            _cache.Put(key, value);
        }

        /// <summary>
        /// Gets an entry from the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>Entry value.</returns>
        public TV Get(TK key)
        {
            return _cache.Get(key);
        }

        /// <summary>
        /// Clears the map.
        /// </summary>
        public void Clear()
        {
            _cache.Clear();
        }

        /// <summary>
        /// Gets the size of the map.
        /// </summary>
        /// <value>
        /// The size.
        /// </value>
        public int Size
        {
            get { return _cache.GetSize(); }
        }
    }
}
