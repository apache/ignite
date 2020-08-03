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
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Unmanaged thread utils.
    /// </summary>
    internal static class UnmanagedThread
    {
        /// <summary>
        /// Delegate for <see cref="SetThreadExitCallback"/>.
        /// </summary>
        /// <param name="threadLocalValue">Value from <see cref="EnableCurrentThreadExitEvent"/></param>
        public delegate void ThreadExitCallback(IntPtr threadLocalValue);

        /// <summary>
        /// Sets the thread exit callback, and returns an id to pass to <see cref="EnableCurrentThreadExitEvent"/>.
        /// </summary>
        /// <param name="callbackPtr">
        /// Pointer to a callback function that matches <see cref="ThreadExitCallback"/>.
        /// </param>
        public static unsafe int SetThreadExitCallback(IntPtr callbackPtr)
        {
            Debug.Assert(callbackPtr != IntPtr.Zero);

            if (Os.IsWindows)
            {
                var res = NativeMethodsWindows.FlsAlloc(callbackPtr);

                if (res == NativeMethodsWindows.FLS_OUT_OF_INDEXES)
                {
                    throw new InvalidOperationException("FlsAlloc failed: " + Marshal.GetLastWin32Error());
                }

                return res;
            }

            if (Os.IsMacOs)
            {
                int tlsIndex;
                var res = NativeMethodsMacOs.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr);

                NativeMethodsLinux.CheckResult(res);

                return tlsIndex;
            }

            if (Os.IsLinux)
            {
                int tlsIndex;
                var res = Os.IsMono
                    ? NativeMethodsMono.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr)
                    : NativeMethodsLinux.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr);

                NativeMethodsLinux.CheckResult(res);

                return tlsIndex;
            }

            throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// NOTE: callback may be called as a result of this method call on some platforms.
        /// </summary>
        /// <param name="callbackId">Callback id returned from <see cref="SetThreadExitCallback"/>.</param>
        public static void RemoveThreadExitCallback(int callbackId)
        {
            if (Os.IsWindows)
            {
                var res = NativeMethodsWindows.FlsFree(callbackId);

                if (!res)
                {
                    throw new InvalidOperationException("FlsFree failed: " + Marshal.GetLastWin32Error());
                }
            }
            else if (Os.IsMacOs)
            {
                var res = NativeMethodsMacOs.pthread_key_delete(callbackId);
                NativeMethodsLinux.CheckResult(res);
            }
            else if (Os.IsLinux)
            {
                var res = Os.IsMono 
                    ? NativeMethodsMono.pthread_key_delete(callbackId)
                    : NativeMethodsLinux.pthread_key_delete(callbackId);
                
                NativeMethodsLinux.CheckResult(res);
            }
            else
            {
                throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
            }
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        public static void EnableCurrentThreadExitEvent(int callbackId, IntPtr threadLocalValue)
        {
            Debug.Assert(threadLocalValue != IntPtr.Zero);

            // Store any value so that destructor callback is fired.
            if (Os.IsWindows)
            {
                var res = NativeMethodsWindows.FlsSetValue(callbackId, threadLocalValue);

                if (!res)
                {
                    throw new InvalidOperationException("FlsSetValue failed: " + Marshal.GetLastWin32Error());
                }
            }
            else if (Os.IsMacOs)
            {
                var res = NativeMethodsMacOs.pthread_setspecific(callbackId, threadLocalValue);
                NativeMethodsLinux.CheckResult(res);
            }
            else if (Os.IsLinux)
            {
                var res = Os.IsMono 
                    ? NativeMethodsMono.pthread_setspecific(callbackId, threadLocalValue)
                    : NativeMethodsLinux.pthread_setspecific(callbackId, threadLocalValue);
                
                NativeMethodsLinux.CheckResult(res);
            }
            else
            {
                throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
            }
        }

        /// <summary>
        /// Windows imports.
        /// </summary>
        private static class NativeMethodsWindows
        {
            // ReSharper disable once InconsistentNaming
            public const int FLS_OUT_OF_INDEXES = -1;

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("kernel32.dll", SetLastError = true)]
            public static extern int FlsAlloc(IntPtr destructorCallback);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("kernel32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool FlsFree(int dwFlsIndex);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("kernel32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool FlsSetValue(int dwFlsIndex, IntPtr lpFlsData);
        }

        /// <summary>
        /// Linux imports.
        /// </summary>
        private static class NativeMethodsLinux
        {
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libcoreclr.so")]
            public static extern int pthread_key_create(IntPtr key, IntPtr destructorCallback);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libcoreclr.so")]
            public static extern int pthread_key_delete(int key);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libcoreclr.so")]
            public static extern int pthread_setspecific(int key, IntPtr value);

            /// <summary>
            /// Checks native call result.
            /// </summary>
            public static void CheckResult(int res)
            {
                if (res != 0)
                {
                    throw new InvalidOperationException("Native call failed: " + res);
                }
            }
        }

        /// <summary>
        /// macOS imports.
        /// </summary>
        private static class NativeMethodsMacOs
        {
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libSystem.dylib")]
            public static extern int pthread_key_create(IntPtr key, IntPtr destructorCallback);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libSystem.dylib")]
            public static extern int pthread_key_delete(int key);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libSystem.dylib")]
            public static extern int pthread_setspecific(int key, IntPtr value);
        }
        
        /// <summary>
        /// Mono on Linux requires __Internal instead of libcoreclr.so.
        /// </summary>
        private static class NativeMethodsMono
        {
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("__Internal")]
            public static extern int pthread_key_create(IntPtr key, IntPtr destructorCallback);
            
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("__Internal")]
            public static extern int pthread_key_delete(int key);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("__Internal")]
            public static extern int pthread_setspecific(int key, IntPtr value);
        }
    }
}
