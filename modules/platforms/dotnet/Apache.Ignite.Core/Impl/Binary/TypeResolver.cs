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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Resolves types by name.
    /// </summary>
    internal class TypeResolver
    {
        /** Assemblies loaded in ReflectionOnly mode. */
        private readonly Dictionary<string, Assembly> _reflectionOnlyAssemblies = new Dictionary<string, Assembly>();

        /// <summary>
        /// Resolve type by name.
        /// </summary>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="assemblyName">Optional, name of the assembly.</param>
        /// <returns>
        /// Resolved type.
        /// </returns>
        public Type ResolveType(string typeName, string assemblyName = null)
        {
            Debug.Assert(!string.IsNullOrEmpty(typeName));

            // Fully-qualified name can be resolved with system mechanism.
            var type = Type.GetType(typeName, false);

            if (type != null)
            {
                return type;
            }

            var parsedType = TypeNameParser.Parse(typeName);

            // Partial names should be resolved by scanning assemblies.
            return ResolveType(assemblyName, parsedType, AppDomain.CurrentDomain.GetAssemblies())
                ?? ResolveTypeInReferencedAssemblies(assemblyName, parsedType);
        }

        /// <summary>
        /// Resolve type by name in specified assembly set.
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="assemblies">Assemblies to look in.</param>
        /// <returns> 
        /// Resolved type. 
        /// </returns>
        private static Type ResolveType(string assemblyName, TypeNameParser typeName, ICollection<Assembly> assemblies)
        {
            var type = ResolveNonGenericType(assemblyName, typeName.GetFullName(), assemblies);

            if (type == null)
            {
                return null;
            }

            if (type.IsGenericTypeDefinition && typeName.Generics != null)
            {
                var genArgs = typeName.Generics.Select(x => ResolveType(assemblyName, x, assemblies)).ToArray();

                return type.MakeGenericType(genArgs);
            }

            return type;
        }

        /// <summary>
        /// Resolves non-generic type by searching provided assemblies.
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="assemblies">The assemblies.</param>
        /// <returns>Resolved type, or null.</returns>
        private static Type ResolveNonGenericType(string assemblyName, string typeName, ICollection<Assembly> assemblies)
        {
            // Fully-qualified name can be resolved with system mechanism.
            var type = Type.GetType(typeName, false);

            if (type != null)
            {
                return type;
            }

            if (!string.IsNullOrEmpty(assemblyName))
            {
                assemblies = assemblies
                    .Where(x => x.FullName == assemblyName || x.GetName().Name == assemblyName).ToArray();
            }

            if (!assemblies.Any())
            {
                return null;
            }

            // Trim assembly qualification
            var commaIdx = typeName.IndexOf(',');

            if (commaIdx > 0)
            {
                typeName = typeName.Substring(0, commaIdx);
            }

            return assemblies.Select(a => a.GetType(typeName, false, false)).FirstOrDefault(x => x != null);
        }

        /// <summary>
        /// Resolve type by name in non-loaded referenced assemblies.
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <returns>
        /// Resolved type.
        /// </returns>
        private Type ResolveTypeInReferencedAssemblies(string assemblyName, TypeNameParser typeName)
        {
            ResolveEventHandler resolver = (sender, args) => GetReflectionOnlyAssembly(args.Name);

            AppDomain.CurrentDomain.ReflectionOnlyAssemblyResolve += resolver;

            try
            {
                var result = ResolveType(assemblyName, typeName, GetNotLoadedReferencedAssemblies().ToArray());

                if (result == null)
                    return null;

                // result is from ReflectionOnly assembly, load it properly into current domain
                var asm = AppDomain.CurrentDomain.Load(result.Assembly.GetName());

                return asm.GetType(result.FullName);
            }
            finally
            {
                AppDomain.CurrentDomain.ReflectionOnlyAssemblyResolve -= resolver;
            }
        }

        /// <summary>
        /// Gets the reflection only assembly.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private Assembly GetReflectionOnlyAssembly(string fullName)
        {
            Assembly result;

            if (!_reflectionOnlyAssemblies.TryGetValue(fullName, out result))
            {
                try
                {
                    result = Assembly.ReflectionOnlyLoad(fullName);
                }
                catch (Exception)
                {
                    // Some assemblies may fail to load
                    result = null;
                }

                _reflectionOnlyAssemblies[fullName] = result;
            }

            return result;
        }

        /// <summary>
        /// Recursively gets all referenced assemblies for current app domain, excluding those that are loaded.
        /// </summary>
        private IEnumerable<Assembly> GetNotLoadedReferencedAssemblies()
        {
            var roots = new Stack<Assembly>(AppDomain.CurrentDomain.GetAssemblies());

            var visited = new HashSet<string>();

            var loaded = new HashSet<string>(roots.Select(x => x.FullName));

            while (roots.Any())
            {
                var asm = roots.Pop();

                if (visited.Contains(asm.FullName))
                    continue;

                if (!loaded.Contains(asm.FullName))
                    yield return asm;

                visited.Add(asm.FullName);

                foreach (var refAsm in asm.GetReferencedAssemblies()
                    .Where(x => !visited.Contains(x.FullName))
                    .Where(x => !loaded.Contains(x.FullName))
                    .Select(x => GetReflectionOnlyAssembly(x.FullName))
                    .Where(x => x != null))
                    roots.Push(refAsm);
            }
        }
    }
}