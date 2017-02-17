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
                throw new IgniteException(string.Format("Failed to load {0}: {1}", 
                    IgniteUtils.FileIgniteJniDll, Marshal.GetLastWin32Error()));

            AppDomain.CurrentDomain.DomainUnload += CurrentDomain_DomainUnload;

            JNI.SetConsoleHandler(UnmanagedCallbacks.ConsoleWriteHandler);
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
                    // OnStart receives the same InteropProcessor as here (just as another GlobalRef) and stores it.
                    // Release current reference immediately.
                    void* res = JNI.IgnitionStart(ctx.NativeContext, cfgPath0, gridName0, InteropFactoryId,
                        mem.SynchronizeOutput());

                    JNI.Release(res);
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

        internal static void IgnitionStopAll(void* ctx, bool cancel)
        {
            JNI.IgnitionStopAll(ctx, cancel);
        }

        internal static void ProcessorReleaseStart(IUnmanagedTarget target)
        {
            JNI.ProcessorReleaseStart(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ProcessorProjection(IUnmanagedTarget target)
        {
            void* res = JNI.ProcessorProjection(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorCache(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorCreateCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorCreateCache(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorCreateCache(IUnmanagedTarget target, long memPtr)
        {
            void* res = JNI.ProcessorCreateCacheFromConfig(target.Context, target.Target, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorGetOrCreateCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorGetOrCreateCache(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorGetOrCreateCache(IUnmanagedTarget target, long memPtr)
        {
            void* res = JNI.ProcessorGetOrCreateCacheFromConfig(target.Context, target.Target, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCreateNearCache(IUnmanagedTarget target, string name, long memPtr)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorCreateNearCache(target.Context, target.Target, name0, memPtr);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorGetOrCreateNearCache(IUnmanagedTarget target, string name, long memPtr)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorGetOrCreateNearCache(target.Context, target.Target, name0, memPtr);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static void ProcessorDestroyCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                JNI.ProcessorDestroyCache(target.Context, target.Target, name0);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorAffinity(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorAffinity(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorDataStreamer(IUnmanagedTarget target, string name, bool keepBinary)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = JNI.ProcessorDataStreamer(target.Context, target.Target, name0, keepBinary);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }
        
        internal static IUnmanagedTarget ProcessorTransactions(IUnmanagedTarget target)
        {
            void* res = JNI.ProcessorTransactions(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCompute(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = JNI.ProcessorCompute(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorMessage(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = JNI.ProcessorMessage(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorEvents(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = JNI.ProcessorEvents(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorServices(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = JNI.ProcessorServices(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorExtensions(IUnmanagedTarget target)
        {
            void* res = JNI.ProcessorExtensions(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorAtomicLong(IUnmanagedTarget target, string name, long initialValue, 
            bool create)
        {
            var name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                var res = JNI.ProcessorAtomicLong(target.Context, target.Target, name0, initialValue, create);

                return res == null ? null : target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorAtomicSequence(IUnmanagedTarget target, string name, long initialValue, 
            bool create)
        {
            var name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                var res = JNI.ProcessorAtomicSequence(target.Context, target.Target, name0, initialValue, create);

                return res == null ? null : target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorAtomicReference(IUnmanagedTarget target, string name, long memPtr, 
            bool create)
        {
            var name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                var res = JNI.ProcessorAtomicReference(target.Context, target.Target, name0, memPtr, create);

                return res == null ? null : target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static void ProcessorGetIgniteConfiguration(IUnmanagedTarget target, long memPtr)
        {
            JNI.ProcessorGetIgniteConfiguration(target.Context, target.Target, memPtr);
        }

        internal static void ProcessorGetCacheNames(IUnmanagedTarget target, long memPtr)
        {
            JNI.ProcessorGetCacheNames(target.Context, target.Target, memPtr);
        }

        internal static bool ProcessorLoggerIsLevelEnabled(IUnmanagedTarget target, int level)
        {
            return JNI.ProcessorLoggerIsLevelEnabled(target.Context, target.Target, level);
        }

        internal static void ProcessorLoggerLog(IUnmanagedTarget target, int level, string message, string category,
            string errorInfo)
        {
            var message0 = IgniteUtils.StringToUtf8Unmanaged(message);
            var category0 = IgniteUtils.StringToUtf8Unmanaged(category);
            var errorInfo0 = IgniteUtils.StringToUtf8Unmanaged(errorInfo);

            try
            {
                JNI.ProcessorLoggerLog(target.Context, target.Target, level, message0, category0, errorInfo0);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(message0));
                Marshal.FreeHGlobal(new IntPtr(category0));
                Marshal.FreeHGlobal(new IntPtr(errorInfo0));
            }
        }

        internal static IUnmanagedTarget ProcessorBinaryProcessor(IUnmanagedTarget target)
        {
            void* res = JNI.ProcessorBinaryProcessor(target.Context, target.Target);

            return target.ChangeTarget(res);
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
