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
        /** */
        private static readonly Func<IntPtr, int> SetThreadExitCallbackDelegate;

        /** */
        private static readonly Action<int> RemoveThreadExitCallbackDelegate;

        /** */
        private static readonly Action<int, IntPtr> EnableCurrentThreadExitEventDelegate;

        /// <summary>
        /// Delegate for <see cref="SetThreadExitCallback"/>.
        /// </summary>
        /// <param name="threadLocalValue">Value from <see cref="EnableCurrentThreadExitEvent"/></param>
        public delegate void ThreadExitCallback(IntPtr threadLocalValue);

        /// <summary>
        /// Initializes the <see cref="UnmanagedThread"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static UnmanagedThread()
        {
            NativeLibraryUtils.SetDllImportResolvers();

            if (Os.IsWindows)
            {
                SetThreadExitCallbackDelegate = SetThreadExitCallbackWindows;
                RemoveThreadExitCallbackDelegate = RemoveThreadExitCallbackWindows;
                EnableCurrentThreadExitEventDelegate = EnableCurrentThreadExitEventWindows;
            }
            else if (Os.IsMacOs)
            {
                SetThreadExitCallbackDelegate = SetThreadExitCallbackMacOs;
                RemoveThreadExitCallbackDelegate = RemoveThreadExitCallbackMacOs;
                EnableCurrentThreadExitEventDelegate = EnableCurrentThreadExitEventMacOs;
            }
            else if (Os.IsLinux)
            {
                if (Os.IsMono)
                {
                    SetThreadExitCallbackDelegate = SetThreadExitCallbackMono;
                    RemoveThreadExitCallbackDelegate = RemoveThreadExitCallbackMono;
                    EnableCurrentThreadExitEventDelegate = EnableCurrentThreadExitEventMono;
                }
                else
                {
                    unsafe
                    {
                        // Depending on the Linux distro, use either libcoreclr or libpthread.
                        try
                        {
                            int tlsIndex;

                            CheckResult(NativeMethodsLinuxLibcoreclr.pthread_key_create(new IntPtr(&tlsIndex), IntPtr.Zero));
                            CheckResult(NativeMethodsLinuxLibcoreclr.pthread_key_delete(tlsIndex));

                            SetThreadExitCallbackDelegate = SetThreadExitCallbackLibcoreclr;
                            RemoveThreadExitCallbackDelegate = RemoveThreadExitCallbackLibcoreclr;
                            EnableCurrentThreadExitEventDelegate = EnableCurrentThreadExitEventLibcoreclr;
                        }
                        catch (EntryPointNotFoundException)
                        {
                            SetThreadExitCallbackDelegate = SetThreadExitCallbackLibpthread;
                            RemoveThreadExitCallbackDelegate = RemoveThreadExitCallbackLibpthread;
                            EnableCurrentThreadExitEventDelegate = EnableCurrentThreadExitEventLibpthread;
                        }
                    }
                }
            }
            else
            {
                throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
            }
        }

        /// <summary>
        /// Sets the thread exit callback, and returns an id to pass to <see cref="EnableCurrentThreadExitEvent"/>.
        /// </summary>
        /// <param name="callbackPtr">
        /// Pointer to a callback function that matches <see cref="ThreadExitCallback"/>.
        /// </param>
        public static int SetThreadExitCallback(IntPtr callbackPtr)
        {
            Debug.Assert(callbackPtr != IntPtr.Zero);

            return SetThreadExitCallbackDelegate(callbackPtr);
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// NOTE: callback may be called as a result of this method call on some platforms.
        /// </summary>
        /// <param name="callbackId">Callback id returned from <see cref="SetThreadExitCallback"/>.</param>
        public static void RemoveThreadExitCallback(int callbackId)
        {
            RemoveThreadExitCallbackDelegate(callbackId);
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        public static void EnableCurrentThreadExitEvent(int callbackId, IntPtr threadLocalValue)
        {
            Debug.Assert(threadLocalValue != IntPtr.Zero);

            EnableCurrentThreadExitEventDelegate(callbackId, threadLocalValue);
        }

        /// <summary>
        /// Sets the thread exit callback.
        /// </summary>
        private static unsafe int SetThreadExitCallbackMacOs(IntPtr callbackPtr)
        {
            int tlsIndex;
            var res = NativeMethodsMacOs.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr);

            CheckResult(res);

            return tlsIndex;
        }

        /// <summary>
        /// Sets the thread exit callback.
        /// </summary>
        private static int SetThreadExitCallbackWindows(IntPtr callbackPtr)
        {
            var res = NativeMethodsWindows.FlsAlloc(callbackPtr);

            if (res == NativeMethodsWindows.FLS_OUT_OF_INDEXES)
            {
                throw new InvalidOperationException("FlsAlloc failed: " + Marshal.GetLastWin32Error());
            }

            return res;
        }

        /// <summary>
        /// Sets the thread exit callback.
        /// </summary>
        private static unsafe int SetThreadExitCallbackMono(IntPtr callbackPtr)
        {
            int tlsIndex;

            CheckResult(NativeMethodsMono.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr));

            return tlsIndex;
        }

        /// <summary>
        /// Sets the thread exit callback.
        /// </summary>
        private static unsafe int SetThreadExitCallbackLibcoreclr(IntPtr callbackPtr)
        {
            int tlsIndex;

            CheckResult(NativeMethodsLinuxLibcoreclr.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr));

            return tlsIndex;
        }

        /// <summary>
        /// Sets the thread exit callback.
        /// </summary>
        private static unsafe int SetThreadExitCallbackLibpthread(IntPtr callbackPtr)
        {
            int tlsIndex;

            CheckResult(NativeMethodsLinuxLibpthread.pthread_key_create(new IntPtr(&tlsIndex), callbackPtr));

            return tlsIndex;
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// </summary>
        private static void RemoveThreadExitCallbackLibpthread(int callbackId)
        {
            CheckResult(NativeMethodsLinuxLibpthread.pthread_key_delete(callbackId));
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// </summary>
        private static void RemoveThreadExitCallbackLibcoreclr(int callbackId)
        {
            CheckResult(NativeMethodsLinuxLibcoreclr.pthread_key_delete(callbackId));
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// </summary>
        private static void RemoveThreadExitCallbackMono(int callbackId)
        {
            CheckResult(NativeMethodsMono.pthread_key_delete(callbackId));
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// </summary>
        private static void RemoveThreadExitCallbackMacOs(int callbackId)
        {
            CheckResult(NativeMethodsMacOs.pthread_key_delete(callbackId));
        }

        /// <summary>
        /// Removes thread exit callback that has been set with <see cref="SetThreadExitCallback"/>.
        /// </summary>
        private static void RemoveThreadExitCallbackWindows(int callbackId)
        {
            var res = NativeMethodsWindows.FlsFree(callbackId);

            if (!res)
            {
                throw new InvalidOperationException("FlsFree failed: " + Marshal.GetLastWin32Error());
            }
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        private static void EnableCurrentThreadExitEventLibpthread(int callbackId, IntPtr threadLocalValue)
        {
            CheckResult(NativeMethodsLinuxLibpthread.pthread_setspecific(callbackId, threadLocalValue));
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        private static void EnableCurrentThreadExitEventLibcoreclr(int callbackId, IntPtr threadLocalValue)
        {
            CheckResult(NativeMethodsLinuxLibcoreclr.pthread_setspecific(callbackId, threadLocalValue));
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        private static void EnableCurrentThreadExitEventMono(int callbackId, IntPtr threadLocalValue)
        {
            CheckResult(NativeMethodsMono.pthread_setspecific(callbackId, threadLocalValue));
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        private static void EnableCurrentThreadExitEventMacOs(int callbackId, IntPtr threadLocalValue)
        {
            CheckResult(NativeMethodsMacOs.pthread_setspecific(callbackId, threadLocalValue));
        }

        /// <summary>
        /// Enables thread exit event for current thread.
        /// </summary>
        private static void EnableCurrentThreadExitEventWindows(int callbackId, IntPtr threadLocalValue)
        {
            var res = NativeMethodsWindows.FlsSetValue(callbackId, threadLocalValue);

            if (!res)
            {
                throw new InvalidOperationException("FlsSetValue failed: " + Marshal.GetLastWin32Error());
            }
        }

        /// <summary>
        /// Checks native call result.
        /// </summary>
        private static void CheckResult(int res)
        {
            if (res != 0)
            {
                throw new InvalidOperationException("Native call failed: " + res);
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
        private static class NativeMethodsLinuxLibcoreclr
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
        }

        /// <summary>
        /// Linux imports.
        /// </summary>
        private static class NativeMethodsLinuxLibpthread
        {
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libpthread.so")]
            public static extern int pthread_key_create(IntPtr key, IntPtr destructorCallback);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libpthread.so")]
            public static extern int pthread_key_delete(int key);

            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass", Justification = "Reviewed.")]
            [DllImport("libpthread.so")]
            public static extern int pthread_setspecific(int key, IntPtr value);
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
