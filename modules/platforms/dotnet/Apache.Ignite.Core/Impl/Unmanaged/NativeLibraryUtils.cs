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

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using System;
    using System.Diagnostics;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Native library call utilities.
    /// </summary>
    public static class NativeLibraryUtils
    {
        /// <summary>
        /// Sets dll import resolvers.
        /// </summary>
        public static void SetDllImportResolvers()
        {
            // Init custom resolver for .NET 5+ single-file apps.
            // Do it with Reflection, because SetDllImportResolver is not available on some frameworks,
            // and multi-targeting is not yet implemented.
            // TODO: Cache all of this.
            var dllImportResolverType = Type.GetType("System.Runtime.InteropServices.DllImportResolver");
            var dllImportSearchPathType = Type.GetType("System.Runtime.InteropServices.DllImportSearchPath");
            var nativeLibraryType = Type.GetType("System.Runtime.InteropServices.NativeLibrary");

            if (dllImportResolverType == null || dllImportSearchPathType == null || nativeLibraryType == null)
            {
                return;
            }

            var setDllImportResolverMethod = nativeLibraryType.GetMethod("SetDllImportResolver");

            if (setDllImportResolverMethod == null)
            {
                return;
            }

            var resolveMethod = typeof(NativeLibraryUtils).GetMethod("Resolve", BindingFlags.Static | BindingFlags.NonPublic);
            Debug.Assert(resolveMethod != null);

            var libraryName = Expression.Parameter(typeof(string));
            var assembly = Expression.Parameter(typeof(Assembly));
            var searchPath = Expression.Parameter(typeof(Nullable<>).MakeGenericType(dllImportSearchPathType));

            var resolve = Expression.Call(resolveMethod, libraryName);
            var dllImportResolver = Expression.Lambda(dllImportResolverType, resolve, libraryName, assembly, searchPath);
            var resolveDelegate = dllImportResolver.Compile();

            setDllImportResolverMethod.Invoke(null, new object[] {typeof(Ignition).Assembly, resolveDelegate});
        }

        /// <summary>
        /// Resolves the native library.
        /// </summary>
        // ReSharper disable once UnusedMember.Local (reflection).
        private static IntPtr Resolve(string libraryName)
        {
            return libraryName == "libcoreclr.so"
                ? (IntPtr) (-1)  // Self-referencing binary.
                : IntPtr.Zero;   // Skip.
        }
    }
}
