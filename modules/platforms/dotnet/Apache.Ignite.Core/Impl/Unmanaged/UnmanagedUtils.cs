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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using JNI = IgniteJniNativeMethods;

    /// <summary>
    /// Unmanaged utility classes.
    /// </summary>
    internal static unsafe class UnmanagedUtils
    {
        /** Interop factory ID for .Net. */
        private const int InteropFactoryId = 1;

        /// <summary>
        /// Initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        static UnmanagedUtils()
        {
            var platform = Environment.Is64BitProcess ? "x64" : "x86";

            var resName = string.Format("{0}.{1}", platform, IgniteUtils.FileIgniteJniDll);

            var path = IgniteUtils.UnpackEmbeddedResource(resName, IgniteUtils.FileIgniteJniDll);

            var ptr = NativeMethods.LoadLibrary(path);

            if (ptr == IntPtr.Zero)
            {
                var err = Marshal.GetLastWin32Error();

                throw new IgniteException(string.Format("Failed to load {0} from {1}: [{2}]",
                    IgniteUtils.FileIgniteJniDll, path, IgniteUtils.FormatWin32Error(err)));
            }

            AppDomain.CurrentDomain.DomainUnload += CurrentDomain_DomainUnload;

            JNI.SetConsoleHandler(UnmanagedCallbacks.ConsoleWriteHandler);

            // Clean directories in background to avoid extra work on start.
            Task.Factory.StartNew(IgniteUtils.TryCleanTempDirectories);
        }

        /// <summary>
        /// Handles the DomainUnload event of the current AppDomain.
        /// </summary>
        private static void CurrentDomain_DomainUnload(object sender, EventArgs e)
        {
            // Clean the handler to avoid JVM crash.
            var removedCnt = JNI.RemoveConsoleHandler(UnmanagedCallbacks.ConsoleWriteHandler);

            Debug.Assert(removedCnt == 1);
        }

        /// <summary>
        /// No-op initializer used to force type loading and static constructor call.
        /// </summary>
        internal static void Initialize()
        {
            // No-op.
        }

        #region NATIVE METHODS: PROCESSOR

        internal static void IgnitionStart(UnmanagedContext ctx, string cfgPath, string gridName,
            bool clientMode, bool userLogger)
        {
            using (var mem = IgniteManager.Memory.Allocate().GetStream())
            {
                mem.WriteBool(clientMode);
                mem.WriteBool(userLogger);

                sbyte* cfgPath0 = IgniteUtils.StringToUtf8Unmanaged(cfgPath);
                sbyte* gridName0 = IgniteUtils.StringToUtf8Unmanaged(gridName);

                try
                {
                    // OnStart receives InteropProcessor referece and stores it.
                    JNI.IgnitionStart(ctx.NativeContext, cfgPath0, gridName0, InteropFactoryId,
                        mem.SynchronizeOutput());
                }
                finally
                {
                    Marshal.FreeHGlobal(new IntPtr(cfgPath0));
                    Marshal.FreeHGlobal(new IntPtr(gridName0));
                }
            }
        }

        internal static bool IgnitionStop(void* ctx, string gridName, bool cancel)
        {
            sbyte* gridName0 = IgniteUtils.StringToUtf8Unmanaged(gridName);

            try
            {
                return JNI.IgnitionStop(ctx, gridName0, cancel);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(gridName0));
            }
        }

        #endregion

        #region NATIVE METHODS: TARGET

        internal static long TargetInLongOutLong(IUnmanagedTarget target, int opType, long memPtr)
        {
            return JNI.TargetInLongOutLong(target.Context, target.Target, opType, memPtr);
        }

        internal static long TargetInStreamOutLong(IUnmanagedTarget target, int opType, long memPtr)
        {
            return JNI.TargetInStreamOutLong(target.Context, target.Target, opType, memPtr);
        }

        internal static void TargetInStreamOutStream(IUnmanagedTarget target, int opType, long inMemPtr, long outMemPtr)
        {
            JNI.TargetInStreamOutStream(target.Context, target.Target, opType, inMemPtr, outMemPtr);
        }

        internal static IUnmanagedTarget TargetInStreamOutObject(IUnmanagedTarget target, int opType, long inMemPtr)
        {
            void* res = JNI.TargetInStreamOutObject(target.Context, target.Target, opType, inMemPtr);

            if (res == null)
                return null;

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget TargetInObjectStreamOutObjectStream(IUnmanagedTarget target, int opType, void* arg, long inMemPtr, long outMemPtr)
        {
            void* res = JNI.TargetInObjectStreamOutObjectStream(target.Context, target.Target, opType, arg, inMemPtr, outMemPtr);

            if (res == null)
                return null;

            return target.ChangeTarget(res);
        }

        internal static void TargetOutStream(IUnmanagedTarget target, int opType, long memPtr)
        {
            JNI.TargetOutStream(target.Context, target.Target, opType, memPtr);
        }

        internal static IUnmanagedTarget TargetOutObject(IUnmanagedTarget target, int opType)
        {
            void* res = JNI.TargetOutObject(target.Context, target.Target, opType);

            return target.ChangeTarget(res);
        }

        internal static void TargetInStreamAsync(IUnmanagedTarget target, int opType, long memPtr)
        {
            JNI.TargetInStreamAsync(target.Context, target.Target, opType, memPtr);
        }

        internal static IUnmanagedTarget TargetInStreamOutObjectAsync(IUnmanagedTarget target, int opType, long memPtr)
        {
            void* res = JNI.TargetInStreamOutObjectAsync(target.Context, target.Target, opType, memPtr);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: MISCELANNEOUS

        internal static void Reallocate(long memPtr, int cap)
        {
            int res = JNI.Reallocate(memPtr, cap);

            if (res != 0)
                throw new IgniteException("Failed to reallocate external memory [ptr=" + memPtr + 
                    ", capacity=" + cap + ']');
        }

        internal static IUnmanagedTarget Acquire(UnmanagedContext ctx, void* target)
        {
            void* target0 = JNI.Acquire(ctx.NativeContext, target);

            return new UnmanagedTarget(ctx, target0);
        }

        internal static void Release(IUnmanagedTarget target)
        {
            JNI.Release(target.Target);
        }

        internal static void ThrowToJava(void* ctx, Exception e)
        {
            char* msgChars = (char*)IgniteUtils.StringToUtf8Unmanaged(e.Message);

            try
            {
                JNI.ThrowToJava(ctx, msgChars);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(msgChars));
            }
        }

        internal static int HandlersSize()
        {
            return JNI.HandlersSize();
        }

        internal static void* CreateContext(void* opts, int optsLen, void* cbs)
        {
            return JNI.CreateContext(opts, optsLen, cbs);
        }

        internal static void DeleteContext(void* ctx)
        {
            JNI.DeleteContext(ctx);
        }

        internal static void DestroyJvm(void* ctx)
        {
            JNI.DestroyJvm(ctx);
        }

        #endregion
    }
}
