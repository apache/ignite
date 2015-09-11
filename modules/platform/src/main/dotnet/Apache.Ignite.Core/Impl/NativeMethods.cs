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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Native methods.
    /// </summary>
    internal static class NativeMethods
    {
        /// <summary>
        /// Load DLL with WinAPI.
        /// </summary>
        /// <param name="path">Path to dll.</param>
        /// <returns></returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, 
            ThrowOnUnmappableChar = true)]
        internal static extern IntPtr LoadLibrary(string path);

        /// <summary>
        /// Get procedure address with WinAPI.
        /// </summary>
        /// <param name="ptr">DLL pointer.</param>
        /// <param name="name">Procedure name.</param>
        /// <returns>Procedure address.</returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, 
            ThrowOnUnmappableChar = true)]
        internal static extern IntPtr GetProcAddress(IntPtr ptr, string name);
    }
}