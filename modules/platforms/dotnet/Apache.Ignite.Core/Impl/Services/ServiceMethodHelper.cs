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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Provides reflection information about types.
    /// This class used by ServiceProxyTypeGenerator and by generated proxy (to initialize static field).
    /// </summary>
    internal static class ServiceMethodHelper
    {
        /// <summary>
        /// Provides information about virtual methods of the type
        /// </summary>
        /// <param name="type">Type to inspect.</param>
        /// <returns>List of virtual methods.</returns>
        public static MethodInfo[] GetVirtualMethods(Type type)
        {
            var methods = new List<MethodInfo>();
            foreach (var method in type.GetMethods(BindingFlags.Instance | BindingFlags.Public |
                                                   BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                if (method.IsVirtual)
                    methods.Add(method);

            if (type.IsInterface)
                foreach (var method in typeof(object).GetMethods(
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
                    if (method.IsVirtual)
                        methods.Add(method);

            return methods.ToArray();
        }
    }
}