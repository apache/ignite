/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Resource
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    using GridGain.Cache.Store;

    /// <summary>
    /// Resource processor.
    /// </summary>
    internal class ResourceProcessor
    {
        /** Mutex. */
        private static readonly object MUX = new object();
        
        /** Cached descriptors. */
        private static volatile IDictionary<Type, ResourceTypeDescriptor> descs = 
            new Dictionary<Type, ResourceTypeDescriptor>();

        /// <summary>
        /// Get descriptor for the given type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns></returns>
        public static ResourceTypeDescriptor Descriptor(Type type)
        {
            IDictionary<Type, ResourceTypeDescriptor> descs0 = descs;

            ResourceTypeDescriptor desc;

            if (!descs0.TryGetValue(type, out desc))
            {
                lock (MUX)
                {
                    if (!descs.TryGetValue(type, out desc))
                    {
                        // Create descriptor from scratch.
                        desc = new ResourceTypeDescriptor(type);

                        descs0 = new Dictionary<Type, ResourceTypeDescriptor>(descs);

                        descs0[type] = desc;

                        descs = descs0;
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
        public static void Inject(object target, GridImpl grid)
        {
            Inject(target, grid.Proxy);
        }

        /// <summary>
        /// Inject resources to the given target.
        /// </summary>
        /// <param name="target">Target object.</param>
        /// <param name="grid">Grid.</param>
        public static void Inject(object target, GridProxy grid)
        {
            if (target != null) {
                var desc = Descriptor(target.GetType());
    
                desc.InjectGrid(target, grid);
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
