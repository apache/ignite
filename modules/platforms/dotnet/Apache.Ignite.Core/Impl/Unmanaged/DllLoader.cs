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
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Dynamically loads unmanaged DLLs with respect to current platform.
    /// </summary>
    internal static class DllLoader
    {
        /** Lazy symbol binding. */
        private const int RtldLazy = 1;

        /** Global symbol access. */
        private const int RtldGlobal = 8;

        /// <summary>
        /// ERROR_BAD_EXE_FORMAT constant.
        /// </summary>
        // ReSharper disable once InconsistentNaming
        private const int ERROR_BAD_EXE_FORMAT = 193;

        /// <summary>
        /// ERROR_MOD_NOT_FOUND constant.
        /// </summary>
        // ReSharper disable once InconsistentNaming
        private const int ERROR_MOD_NOT_FOUND = 126;

        /// <summary>
        /// Loads specified DLL.
        /// </summary>
        /// <returns>Null when successful; error message otherwise.</returns>
        public static string Load(string dllPath)
        {
            if (Os.IsWindows)
            {
                return NativeMethodsWindows.LoadLibrary(dllPath) == IntPtr.Zero 
                    ? FormatWin32Error(Marshal.GetLastWin32Error()) ?? "Unknown error"
                    : null;
            }

            if (Os.IsLinux)
            {
                if (Os.IsMono)
                {
                    return NativeMethodsMono.dlopen(dllPath, RtldGlobal | RtldLazy) == IntPtr.Zero
                        ? GetErrorText(NativeMethodsMono.dlerror())
                        : null;
                }

                if (Os.IsNetCore)
                {
                    return NativeMethodsCore.dlopen(dllPath, RtldGlobal | RtldLazy) == IntPtr.Zero
                        ? GetErrorText(NativeMethodsCore.dlerror())
                        : null;
                }

                return NativeMethodsLinux.dlopen(dllPath, RtldGlobal | RtldLazy) == IntPtr.Zero
                    ? GetErrorText(NativeMethodsLinux.dlerror())
                    : null;
            }

            throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
        }

        /// <summary>
        /// Gets the error text.
        /// </summary>
        private static string GetErrorText(IntPtr charPtr)
        {
            return Marshal.PtrToStringAnsi(charPtr) ?? "Unknown error";
        }

        /// <summary>
        /// Formats the Win32 error.
        /// </summary>
        [ExcludeFromCodeCoverage]
        private static string FormatWin32Error(int errorCode)
        {
            if (errorCode == ERROR_BAD_EXE_FORMAT)
            {
                var mode = Environment.Is64BitProcess ? "x64" : "x86";

                return string.Format("DLL could not be loaded (193: ERROR_BAD_EXE_FORMAT). " +
                                     "This is often caused by x64/x86 mismatch. " +
                                     "Current process runs in {0} mode, and DLL is not {0}.", mode);
            }

            if (errorCode == ERROR_MOD_NOT_FOUND)
            {
                return "DLL could not be loaded (126: ERROR_MOD_NOT_FOUND). " +
                       "This can be caused by missing dependencies. ";
            }

            return string.Format("{0}: {1}", errorCode, new Win32Exception(errorCode).Message);
        }

        /// <summary>
        /// Windows.
        /// </summary>
        private static class NativeMethodsWindows
        {
            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr LoadLibrary(string filename);
        }

        /// <summary>
        /// Linux.
        /// </summary>
        private static class NativeMethodsLinux
        {
            [DllImport("libdl.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlopen(string filename, int flags);

            [DllImport("libdl.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlerror();
        }

        /// <summary>
        /// libdl.so depends on libc6-dev on Linux, use Mono instead.
        /// </summary>
        private static class NativeMethodsMono
        {
            [DllImport("__Internal", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlopen(string filename, int flags);

            [DllImport("__Internal", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlerror();
        }

        /// <summary>
        /// libdl.so depends on libc6-dev on Linux, use libcoreclr instead.
        /// </summary>
        private static class NativeMethodsCore
        {
            [DllImport("libcoreclr.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlopen(string filename, int flags);

            [DllImport("libcoreclr.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlerror();
        }
    }
}