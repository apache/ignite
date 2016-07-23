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
            var platfrom = Environment.Is64BitProcess ? "x64" : "x86";

            var resName = string.Format("{0}.{1}", platfrom, IgniteUtils.FileIgniteJniDll);

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
            bool clientMode)
        {
            using (var mem = IgniteManager.Memory.Allocate().GetStream())
            {
                mem.WriteBool(clientMode);

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

        #endregion

        #region NATIVE METHODS: TARGET

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
            void* res = JNI.TargetInStreanOutObject(target.Context, target.Target, opType, inMemPtr);

            return target.ChangeTarget(res);
        }

        internal static void TargetInObjectStreamOutStream(IUnmanagedTarget target, int opType, void* arg, long inMemPtr, long outMemPtr)
        {
            JNI.TargetInObjectStreamOutStream(target.Context, target.Target, opType, arg, inMemPtr, outMemPtr);
        }

        internal static long TargetOutLong(IUnmanagedTarget target, int opType)
        {
            return JNI.TargetOutLong(target.Context, target.Target, opType);
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

        internal static void TargetListenFuture(IUnmanagedTarget target, long futId, int typ)
        {
            JNI.TargetListenFut(target.Context, target.Target, futId, typ);
        }

        internal static void TargetListenFutureForOperation(IUnmanagedTarget target, long futId, int typ, int opId)
        {
            JNI.TargetListenFutForOp(target.Context, target.Target, futId, typ, opId);
        }

        internal static IUnmanagedTarget TargetListenFutureAndGet(IUnmanagedTarget target, long futId, int typ)
        {
            var res = JNI.TargetListenFutAndGet(target.Context, target.Target, futId, typ);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget TargetListenFutureForOperationAndGet(IUnmanagedTarget target, long futId,
            int typ, int opId)
        {
            var res = JNI.TargetListenFutForOpAndGet(target.Context, target.Target, futId, typ, opId);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: AFFINITY

        internal static int AffinityPartitions(IUnmanagedTarget target)
        {
            return JNI.AffinityParts(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: CACHE

        internal static IUnmanagedTarget CacheWithSkipStore(IUnmanagedTarget target)
        {
            void* res = JNI.CacheWithSkipStore(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithNoRetries(IUnmanagedTarget target)
        {
            void* res = JNI.CacheWithNoRetries(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithExpiryPolicy(IUnmanagedTarget target, long create, long update, long access)
        {
            void* res = JNI.CacheWithExpiryPolicy(target.Context, target.Target, create, update, access);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithAsync(IUnmanagedTarget target)
        {
            void* res = JNI.CacheWithAsync(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithKeepBinary(IUnmanagedTarget target)
        {
            void* res = JNI.CacheWithKeepBinary(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static void CacheClear(IUnmanagedTarget target)
        {
            JNI.CacheClear(target.Context, target.Target);
        }

        internal static void CacheRemoveAll(IUnmanagedTarget target)
        {
            JNI.CacheRemoveAll(target.Context, target.Target);
        }

        internal static IUnmanagedTarget CacheOutOpQueryCursor(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = JNI.CacheOutOpQueryCursor(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheOutOpContinuousQuery(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = JNI.CacheOutOpContinuousQuery(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheIterator(IUnmanagedTarget target)
        {
            void* res = JNI.CacheIterator(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheLocalIterator(IUnmanagedTarget target, int peekModes)
        {
            void* res = JNI.CacheLocalIterator(target.Context, target.Target, peekModes);

            return target.ChangeTarget(res);
        }

        internal static void CacheEnterLock(IUnmanagedTarget target, long id)
        {
            JNI.CacheEnterLock(target.Context, target.Target, id);
        }

        internal static void CacheExitLock(IUnmanagedTarget target, long id)
        {
            JNI.CacheExitLock(target.Context, target.Target, id);
        }

        internal static bool CacheTryEnterLock(IUnmanagedTarget target, long id, long timeout)
        {
            return JNI.CacheTryEnterLock(target.Context, target.Target, id, timeout);
        }

        internal static void CacheCloseLock(IUnmanagedTarget target, long id)
        {
            JNI.CacheCloseLock(target.Context, target.Target, id);
        }

        internal static void CacheRebalance(IUnmanagedTarget target, long futId)
        {
            JNI.CacheRebalance(target.Context, target.Target, futId);
        }

        internal static void CacheStoreCallbackInvoke(IUnmanagedTarget target, long memPtr)
        {
            JNI.CacheStoreCallbackInvoke(target.Context, target.Target, memPtr);
        }

        internal static int CacheSize(IUnmanagedTarget target, int modes, bool loc)
        {
            return JNI.CacheSize(target.Context, target.Target, modes, loc);
        }

        #endregion

        #region NATIVE METHODS: COMPUTE

        internal static void ComputeWithNoFailover(IUnmanagedTarget target)
        {
            JNI.ComputeWithNoFailover(target.Context, target.Target);
        }

        internal static void ComputeWithTimeout(IUnmanagedTarget target, long timeout)
        {
            JNI.ComputeWithTimeout(target.Context, target.Target, timeout);
        }

        internal static IUnmanagedTarget ComputeExecuteNative(IUnmanagedTarget target, long taskPtr, long topVer)
        {
            void* res = JNI.ComputeExecuteNative(target.Context, target.Target, taskPtr, topVer);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: CONTINUOUS QUERY

        internal static void ContinuousQueryClose(IUnmanagedTarget target)
        {
            JNI.ContinuousQryClose(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ContinuousQueryGetInitialQueryCursor(IUnmanagedTarget target)
        {
            void* res = JNI.ContinuousQryGetInitialQueryCursor(target.Context, target.Target);

            return res == null ? null : target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: DATA STREAMER

        internal static void DataStreamerListenTopology(IUnmanagedTarget target, long ptr)
        {
            JNI.DataStreamerListenTop(target.Context, target.Target, ptr);
        }

        internal static bool DataStreamerAllowOverwriteGet(IUnmanagedTarget target)
        {
            return JNI.DataStreamerAllowOverwriteGet(target.Context, target.Target);
        }

        internal static void DataStreamerAllowOverwriteSet(IUnmanagedTarget target, bool val)
        {
            JNI.DataStreamerAllowOverwriteSet(target.Context, target.Target, val);
        }

        internal static bool DataStreamerSkipStoreGet(IUnmanagedTarget target)
        {
            return JNI.DataStreamerSkipStoreGet(target.Context, target.Target);
        }

        internal static void DataStreamerSkipStoreSet(IUnmanagedTarget target, bool val)
        {
            JNI.DataStreamerSkipStoreSet(target.Context, target.Target, val);
        }

        internal static int DataStreamerPerNodeBufferSizeGet(IUnmanagedTarget target)
        {
            return JNI.DataStreamerPerNodeBufferSizeGet(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeBufferSizeSet(IUnmanagedTarget target, int val)
        {
            JNI.DataStreamerPerNodeBufferSizeSet(target.Context, target.Target, val);
        }

        internal static int DataStreamerPerNodeParallelOperationsGet(IUnmanagedTarget target)
        {
            return JNI.DataStreamerPerNodeParallelOpsGet(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeParallelOperationsSet(IUnmanagedTarget target, int val)
        {
            JNI.DataStreamerPerNodeParallelOpsSet(target.Context, target.Target, val);
        }

        #endregion

        #region NATIVE METHODS: MESSAGING

        internal static IUnmanagedTarget MessagingWithASync(IUnmanagedTarget target)
        {
            void* res = JNI.MessagingWithAsync(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: PROJECTION

        internal static IUnmanagedTarget ProjectionForOthers(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = JNI.ProjectionForOthers(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForRemotes(IUnmanagedTarget target)
        {
            void* res = JNI.ProjectionForRemotes(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForDaemons(IUnmanagedTarget target)
        {
            void* res = JNI.ProjectionForDaemons(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForRandom(IUnmanagedTarget target)
        {
            void* res = JNI.ProjectionForRandom(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForOldest(IUnmanagedTarget target)
        {
            void* res = JNI.ProjectionForOldest(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForYoungest(IUnmanagedTarget target)
        {
            void* res = JNI.ProjectionForYoungest(target.Context, target.Target);

            return target.ChangeTarget(res);
        }
        
        internal static IUnmanagedTarget ProjectionForServers(IUnmanagedTarget target)
        {
            void* res = JNI.ProjectionForServers(target.Context, target.Target);

            return target.ChangeTarget(res);
        }
        
        internal static void ProjectionResetMetrics(IUnmanagedTarget target)
        {
            JNI.ProjectionResetMetrics(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ProjectionOutOpRet(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = JNI.ProjectionOutOpRet(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: QUERY CURSOR

        internal static void QueryCursorIterator(IUnmanagedTarget target)
        {
            JNI.QryCursorIterator(target.Context, target.Target);
        }

        internal static void QueryCursorClose(IUnmanagedTarget target)
        {
            JNI.QryCursorClose(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: TRANSACTIONS

        internal static long TransactionsStart(IUnmanagedTarget target, int concurrency, int isolation, long timeout, int txSize)
        {
            return JNI.TxStart(target.Context, target.Target, concurrency, isolation, timeout, txSize);
        }

        internal static int TransactionsCommit(IUnmanagedTarget target, long id)
        {
            return JNI.TxCommit(target.Context, target.Target, id);
        }

        internal static void TransactionsCommitAsync(IUnmanagedTarget target, long id, long futId)
        {
            JNI.TxCommitAsync(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsRollback(IUnmanagedTarget target, long id)
        {
            return JNI.TxRollback(target.Context, target.Target, id);
        }

        internal static void TransactionsRollbackAsync(IUnmanagedTarget target, long id, long futId)
        {
            JNI.TxRollbackAsync(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsClose(IUnmanagedTarget target, long id)
        {
            return JNI.TxClose(target.Context, target.Target, id);
        }

        internal static int TransactionsState(IUnmanagedTarget target, long id)
        {
            return JNI.TxState(target.Context, target.Target, id);
        }

        internal static bool TransactionsSetRollbackOnly(IUnmanagedTarget target, long id)
        {
            return JNI.TxSetRollbackOnly(target.Context, target.Target, id);
        }

        internal static void TransactionsResetMetrics(IUnmanagedTarget target)
        {
            JNI.TxResetMetrics(target.Context, target.Target);
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

        #region NATIVE METHODS: EVENTS

        internal static IUnmanagedTarget EventsWithAsync(IUnmanagedTarget target)
        {
            return target.ChangeTarget(JNI.EventsWithAsync(target.Context, target.Target));
        }

        internal static bool EventsStopLocalListen(IUnmanagedTarget target, long handle)
        {
            return JNI.EventsStopLocalListen(target.Context, target.Target, handle);
        }

        internal static bool EventsIsEnabled(IUnmanagedTarget target, int type)
        {
            return JNI.EventsIsEnabled(target.Context, target.Target, type);
        }

        internal static void EventsLocalListen(IUnmanagedTarget target, long handle, int type)
        {
            JNI.EventsLocalListen(target.Context, target.Target, handle, type);
        }

        #endregion

        #region NATIVE METHODS: SERVICES

        internal static IUnmanagedTarget ServicesWithAsync(IUnmanagedTarget target)
        {
            return target.ChangeTarget(JNI.ServicesWithAsync(target.Context, target.Target));
        }

        internal static IUnmanagedTarget ServicesWithServerKeepBinary(IUnmanagedTarget target)
        {
            return target.ChangeTarget(JNI.ServicesWithServerKeepBinary(target.Context, target.Target));
        }

        internal static void ServicesCancel(IUnmanagedTarget target, string name)
        {
            var nameChars = (char*)IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                JNI.ServicesCancel(target.Context, target.Target, nameChars);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(nameChars));
            }
        }

        internal static void ServicesCancelAll(IUnmanagedTarget target)
        {
            JNI.ServicesCancelAll(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ServicesGetServiceProxy(IUnmanagedTarget target, string name, bool sticky)
        {
            var nameChars = (char*)IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                return target.ChangeTarget(JNI.ServicesGetServiceProxy(target.Context, target.Target, nameChars, sticky));
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(nameChars));
            }
        }

        #endregion

        #region NATIVE METHODS: DATA STRUCTURES

        internal static long AtomicLongGet(IUnmanagedTarget target)
        {
            return JNI.AtomicLongGet(target.Context, target.Target);
        }

        internal static long AtomicLongIncrementAndGet(IUnmanagedTarget target)
        {
            return JNI.AtomicLongIncrementAndGet(target.Context, target.Target);
        }

        internal static long AtomicLongAddAndGet(IUnmanagedTarget target, long value)
        {
            return JNI.AtomicLongAddAndGet(target.Context, target.Target, value);
        }

        internal static long AtomicLongDecrementAndGet(IUnmanagedTarget target)
        {
            return JNI.AtomicLongDecrementAndGet(target.Context, target.Target);
        }

        internal static long AtomicLongGetAndSet(IUnmanagedTarget target, long value)
        {
            return JNI.AtomicLongGetAndSet(target.Context, target.Target, value);
        }

        internal static long AtomicLongCompareAndSetAndGet(IUnmanagedTarget target, long expVal, long newVal)
        {
            return JNI.AtomicLongCompareAndSetAndGet(target.Context, target.Target, expVal, newVal);
        }

        internal static bool AtomicLongIsClosed(IUnmanagedTarget target)
        {
            return JNI.AtomicLongIsClosed(target.Context, target.Target);
        }

        internal static void AtomicLongClose(IUnmanagedTarget target)
        {
            JNI.AtomicLongClose(target.Context, target.Target);
        }

        internal static long AtomicSequenceGet(IUnmanagedTarget target)
        {
            return JNI.AtomicSequenceGet(target.Context, target.Target);
        }

        internal static long AtomicSequenceIncrementAndGet(IUnmanagedTarget target)
        {
            return JNI.AtomicSequenceIncrementAndGet(target.Context, target.Target);
        }

        internal static long AtomicSequenceAddAndGet(IUnmanagedTarget target, long value)
        {
            return JNI.AtomicSequenceAddAndGet(target.Context, target.Target, value);
        }

        internal static int AtomicSequenceGetBatchSize(IUnmanagedTarget target)
        {
            return JNI.AtomicSequenceGetBatchSize(target.Context, target.Target);
        }

        internal static void AtomicSequenceSetBatchSize(IUnmanagedTarget target, int size)
        {
            JNI.AtomicSequenceSetBatchSize(target.Context, target.Target, size);
        }

        internal static bool AtomicSequenceIsClosed(IUnmanagedTarget target)
        {
            return JNI.AtomicSequenceIsClosed(target.Context, target.Target);
        }

        internal static void AtomicSequenceClose(IUnmanagedTarget target)
        {
            JNI.AtomicSequenceClose(target.Context, target.Target);
        }

        internal static bool AtomicReferenceIsClosed(IUnmanagedTarget target)
        {
            return JNI.AtomicReferenceIsClosed(target.Context, target.Target);
        }

        internal static void AtomicReferenceClose(IUnmanagedTarget target)
        {
            JNI.AtomicReferenceClose(target.Context, target.Target);
        }

        internal static bool ListenableCancel(IUnmanagedTarget target)
        {
            return JNI.ListenableCancel(target.Context, target.Target);
        }

        internal static bool ListenableIsCancelled(IUnmanagedTarget target)
        {
            return JNI.ListenableIsCancelled(target.Context, target.Target);
        }

        #endregion
    }
}
