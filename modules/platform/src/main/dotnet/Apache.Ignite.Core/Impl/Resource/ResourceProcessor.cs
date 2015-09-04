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

namespace Apache.Ignite.Core.Impl.Resource
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Store;

    /// <summary>
    /// Resource processor.
    /// </summary>
    internal class ResourceProcessor
    {
        /** Mutex. */
        private static readonly object Mux = new object();
        
        /** Cached descriptors. */
        private static volatile IDictionary<Type, ResourceTypeDescriptor> _descs = 
            new Dictionary<Type, ResourceTypeDescriptor>();

        /// <summary>
        /// Get descriptor for the given type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns></returns>
        public static ResourceTypeDescriptor Descriptor(Type type)
        {
            IDictionary<Type, ResourceTypeDescriptor> descs0 = _descs;

            ResourceTypeDescriptor desc;

            if (!descs0.TryGetValue(type, out desc))
            {
                lock (Mux)
                {
                    if (!_descs.TryGetValue(type, out desc))
                    {
                        // Create descriptor from scratch.
                        desc = new ResourceTypeDescriptor(type);

                        descs0 = new Dictionary<Type, ResourceTypeDescriptor>(_descs);

                        descs0[type] = desc;

                        _descs = descs0;
                    }
                }
            }

            return desc;
        }

        /// <summary>
        /// Inject resources to the given target.
        /// </summary>
        /// <param name="target">Target object.</param>
        /// <param name="grid">Grid.</param>
        public static void Inject(object target, Ignite grid)
        {
            Inject(target, grid.Proxy);
        }

        /// <summary>
        /// Inject resources to the given target.
        /// </summary>
        /// <param name="target">Target object.</param>
        /// <param name="grid">Grid.</param>
        public static void Inject(object target, IgniteProxy grid)
        {
            if (target != null) {
                var desc = Descriptor(target.GetType());
    
                desc.InjectIgnite(target, grid);
            }
        }

        /// <summary>
        /// Inject cache store session.
        /// </summary>
        /// <param name="store">Store.</param>
        /// <param name="ses">Store session.</param>
        public static void InjectStoreSession(ICacheStore store, ICacheStoreSession ses)
        {
            Debug.Assert(store != null);

            Descriptor(store.GetType()).InjectStoreSession(store, ses);
        }
    }
}
