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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;

    /// <summary>
    /// Resolves loaded assemblies by name.
    /// </summary>
    public class LoadedAssembliesResolver
    {
        // The lazy singleton instance.
        private static readonly Lazy<LoadedAssembliesResolver> LazyInstance = new Lazy<LoadedAssembliesResolver>();

        // Assemblies map.
        private volatile Dictionary<string, Assembly> _map;

        /// <summary>
        /// Initializes a new instance of the <see cref="LoadedAssembliesResolver"/> class.
        /// </summary>
        public LoadedAssembliesResolver()
        {
            lock (this)
            {
                AppDomain.CurrentDomain.AssemblyLoad += CurrentDomain_AssemblyLoad;

                UpdateMap();
            }
        }

        /// <summary>
        /// Handles the AssemblyLoad event of the AppDomain.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="args">The <see cref="AssemblyLoadEventArgs"/> instance containing the event data.</param>
        private void CurrentDomain_AssemblyLoad(object sender, AssemblyLoadEventArgs args)
        {
            lock (this)
            {
                UpdateMap();
            }
        }

        /// <summary>
        /// Updates the assembly map according to the current list of loaded assemblies.
        /// </summary>
        private void UpdateMap()
        {
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();

            _map = new Dictionary<string, Assembly>(assemblies.Length);

            foreach (var assembly in assemblies)
                _map[assembly.FullName] = assembly;
        }

        /// <summary>
        /// Gets the singleton instance.
        /// </summary>
        public static LoadedAssembliesResolver Instance
        {
            get { return LazyInstance.Value; }
        }

        /// <summary>
        /// Gets the assembly by name.
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <returns>Assembly with specified name, or null.</returns>
        [SuppressMessage("ReSharper", "InconsistentlySynchronizedField")]
        public Assembly GetAssembly(string assemblyName)
        {
            Assembly asm;

            return _map.TryGetValue(assemblyName, out asm) ? asm : null;
        }
    }
}