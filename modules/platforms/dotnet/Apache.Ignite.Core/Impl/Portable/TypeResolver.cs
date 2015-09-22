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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Resolves types by name.
    /// </summary>
    internal class TypeResolver
    {
        /** Regex to parse generic types from portable configuration. Allows nested generics in type arguments. */
        private static readonly Regex GenericTypeRegex =
            new Regex(@"([^`,\[\]]*)(?:`[0-9]+)?(?:\[((?:(?<br>\[)|(?<-br>\])|[^\[\]]*)+)\])?", RegexOptions.Compiled);

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

            return ResolveType(assemblyName, typeName, AppDomain.CurrentDomain.GetAssemblies())
                ?? ResolveTypeInReferencedAssemblies(assemblyName, typeName);
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
        private static Type ResolveType(string assemblyName, string typeName, ICollection<Assembly> assemblies)
        {
            return ResolveGenericType(assemblyName, typeName, assemblies) ??
                   ResolveNonGenericType(assemblyName, typeName, assemblies);
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
            if (!string.IsNullOrEmpty(assemblyName))
                assemblies = assemblies
                    .Where(x => x.FullName == assemblyName || x.GetName().Name == assemblyName).ToArray();

            if (!assemblies.Any())
                return null;

            // Trim assembly qualification
            var commaIdx = typeName.IndexOf(',');

            if (commaIdx > 0)
                typeName = typeName.Substring(0, commaIdx);

            return assemblies.Select(a => a.GetType(typeName, false, false)).FirstOrDefault(type => type != null);
        }

        /// <summary>
        /// Resolves the name of the generic type by resolving each generic arg separately 
        /// and substituting it's fully qualified name.
        /// (Assembly.GetType finds generic types only when arguments are fully qualified).
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="assemblies">Assemblies</param>
        /// <returns>Fully qualified generic type name, or null if argument(s) could not be resolved.</returns>
        private static Type ResolveGenericType(string assemblyName, string typeName, ICollection<Assembly> assemblies)
        {
            var match = GenericTypeRegex.Match(typeName);

            if (!match.Success || !match.Groups[2].Success)
                return null;

            // Try to construct generic type; each generic arg can also be a generic type.
            var genericArgs = GenericTypeRegex.Matches(match.Groups[2].Value)
                .OfType<Match>().Select(m => m.Value).Where(v => !string.IsNullOrWhiteSpace(v))
                .Select(v => ResolveType(null, TrimBrackets(v), assemblies)).ToArray();

            if (genericArgs.Any(x => x == null))
                return null;

            var genericType = ResolveNonGenericType(assemblyName,
                string.Format("{0}`{1}", match.Groups[1].Value, genericArgs.Length), assemblies);

            if (genericType == null)
                return null;

            return genericType.MakeGenericType(genericArgs);
        }

        /// <summary>
        /// Trims the brackets from generic type arg.
        /// </summary>
        private static string TrimBrackets(string s)
        {
            return s.StartsWith("[") && s.EndsWith("]") ? s.Substring(1, s.Length - 2) : s;
        }

        /// <summary>
        /// Resolve type by name in non-loaded referenced assemblies.
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <returns>
        /// Resolved type.
        /// </returns>
        private Type ResolveTypeInReferencedAssemblies(string assemblyName, string typeName)
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