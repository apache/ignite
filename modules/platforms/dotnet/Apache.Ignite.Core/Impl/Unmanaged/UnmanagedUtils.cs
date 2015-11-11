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
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;
    using System.Security;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Unmanaged utility classes.
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal static unsafe class UnmanagedUtils
    {
        /** Interop factory ID for .Net. */
        private const int InteropFactoryId = 1;

        /// <summary>
        /// Initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static UnmanagedUtils()
        {
            var path = IgniteUtils.UnpackEmbeddedResource(IgniteUtils.FileIgniteJniDll);

            var ptr = NativeMethods.LoadLibrary(path);

            if (ptr == IntPtr.Zero)
                throw new IgniteException("Failed to load " + IgniteUtils.FileIgniteJniDll + ": " + Marshal.GetLastWin32Error());

        }

        /// <summary>
        /// No-op initializer used to force type loading and static constructor call.
        /// </summary>
        internal static void Initialize()
        {
            // No-op.
        }

        #region NATIVE METHODS: PROCESSOR

        internal static IUnmanagedTarget IgnitionStart(UnmanagedContext ctx, string cfgPath, string gridName,
            bool clientMode)
        {
            using (var mem = IgniteManager.Memory.Allocate().GetStream())
            {
                mem.WriteBool(clientMode);

                sbyte* cfgPath0 = IgniteUtils.StringToUtf8Unmanaged(cfgPath);
                sbyte* gridName0 = IgniteUtils.StringToUtf8Unmanaged(gridName);

                try
                {
                    void* res = IgniteJni.IGNITION_START(ctx.NativeContext, cfgPath0, gridName0, InteropFactoryId,
                        mem.SynchronizeOutput());

                    return new UnmanagedTarget(ctx, res);
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
                return IgniteJni.IGNITION_STOP(ctx, gridName0, cancel);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(gridName0));
            }
        }

        internal static void IgnitionStopAll(void* ctx, bool cancel)
        {
            IgniteJni.IGNITION_STOP_ALL(ctx, cancel);
        }
        
        internal static void ProcessorReleaseStart(IUnmanagedTarget target)
        {
            IgniteJni.PROCESSOR_RELEASE_START(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ProcessorProjection(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROCESSOR_PROJECTION(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = IgniteJni.PROCESSOR_CACHE(target.Context, target.Target, name0);

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
                void* res = IgniteJni.PROCESSOR_CREATE_CACHE(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorGetOrCreateCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = IgniteJni.PROCESSOR_GET_OR_CREATE_CACHE(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
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
                void* res = IgniteJni.PROCESSOR_AFFINITY(target.Context, target.Target, name0);

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
                void* res = IgniteJni.PROCESSOR_DATA_STREAMER(target.Context, target.Target, name0, keepBinary);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }
        
        internal static IUnmanagedTarget ProcessorTransactions(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROCESSOR_TRANSACTIONS(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCompute(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = IgniteJni.PROCESSOR_COMPUTE(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorMessage(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = IgniteJni.PROCESSOR_MESSAGE(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorEvents(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = IgniteJni.PROCESSOR_EVENTS(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorServices(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = IgniteJni.PROCESSOR_SERVICES(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorExtensions(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROCESSOR_EXTENSIONS(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorAtomicLong(IUnmanagedTarget target, string name, long initialValue, 
            bool create)
        {
            var name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                var res = IgniteJni.PROCESSOR_ATOMIC_LONG(target.Context, target.Target, name0, initialValue, create);

                return res == null ? null : target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        #endregion

        #region NATIVE METHODS: TARGET

        internal static long TargetInStreamOutLong(IUnmanagedTarget target, int opType, long memPtr)
        {
            return IgniteJni.TARGET_IN_STREAM_OUT_LONG(target.Context, target.Target, opType, memPtr);
        }

        internal static void TargetInStreamOutStream(IUnmanagedTarget target, int opType, long inMemPtr, long outMemPtr)
        {
            IgniteJni.TARGET_IN_STREAM_OUT_STREAM(target.Context, target.Target, opType, inMemPtr, outMemPtr);
        }

        internal static IUnmanagedTarget TargetInStreamOutObject(IUnmanagedTarget target, int opType, long inMemPtr)
        {
            void* res = IgniteJni.TARGET_IN_STREAM_OUT_OBJECT(target.Context, target.Target, opType, inMemPtr);

            return target.ChangeTarget(res);
        }

        internal static void TargetInObjectStreamOutStream(IUnmanagedTarget target, int opType, void* arg, long inMemPtr, long outMemPtr)
        {
            IgniteJni.TARGET_IN_OBJECT_STREAM_OUT_STREAM(target.Context, target.Target, opType, arg, inMemPtr, outMemPtr);
        }

        internal static long TargetOutLong(IUnmanagedTarget target, int opType)
        {
            return IgniteJni.TARGET_OUT_LONG(target.Context, target.Target, opType);
        }

        internal static void TargetOutStream(IUnmanagedTarget target, int opType, long memPtr)
        {
            IgniteJni.TARGET_OUT_STREAM(target.Context, target.Target, opType, memPtr);
        }

        internal static IUnmanagedTarget TargetOutObject(IUnmanagedTarget target, int opType)
        {
            void* res = IgniteJni.TARGET_OUT_OBJECT(target.Context, target.Target, opType);

            return target.ChangeTarget(res);
        }

        internal static void TargetListenFuture(IUnmanagedTarget target, long futId, int typ)
        {
            IgniteJni.TargetListenFut(target.Context, target.Target, futId, typ);
        }

        internal static void TargetListenFutureForOperation(IUnmanagedTarget target, long futId, int typ, int opId)
        {
            IgniteJni.TargetListenFutForOp(target.Context, target.Target, futId, typ, opId);
        }

        #endregion

        #region NATIVE METHODS: AFFINITY

        internal static int AffinityPartitions(IUnmanagedTarget target)
        {
            return IgniteJni.AffinityParts(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: CACHE

        internal static IUnmanagedTarget CacheWithSkipStore(IUnmanagedTarget target)
        {
            void* res = IgniteJni.CACHE_WITH_SKIP_STORE(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithNoRetries(IUnmanagedTarget target)
        {
            void* res = IgniteJni.CACHE_WITH_NO_RETRIES(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithExpiryPolicy(IUnmanagedTarget target, long create, long update, long access)
        {
            void* res = IgniteJni.CACHE_WITH_EXPIRY_POLICY(target.Context, target.Target, create, update, access);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithAsync(IUnmanagedTarget target)
        {
            void* res = IgniteJni.CACHE_WITH_ASYNC(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithKeepBinary(IUnmanagedTarget target)
        {
            void* res = IgniteJni.CACHE_WITH_KEEP_BINARY(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static void CacheClear(IUnmanagedTarget target)
        {
            IgniteJni.CACHE_CLEAR(target.Context, target.Target);
        }

        internal static void CacheRemoveAll(IUnmanagedTarget target)
        {
            IgniteJni.CACHE_REMOVE_ALL(target.Context, target.Target);
        }

        internal static IUnmanagedTarget CacheOutOpQueryCursor(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = IgniteJni.CACHE_OUT_OP_QUERY_CURSOR(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheOutOpContinuousQuery(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = IgniteJni.CACHE_OUT_OP_CONTINUOUS_QUERY(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheIterator(IUnmanagedTarget target)
        {
            void* res = IgniteJni.CACHE_ITERATOR(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheLocalIterator(IUnmanagedTarget target, int peekModes)
        {
            void* res = IgniteJni.CACHE_LOCAL_ITERATOR(target.Context, target.Target, peekModes);

            return target.ChangeTarget(res);
        }

        internal static void CacheEnterLock(IUnmanagedTarget target, long id)
        {
            IgniteJni.CACHE_ENTER_LOCK(target.Context, target.Target, id);
        }

        internal static void CacheExitLock(IUnmanagedTarget target, long id)
        {
            IgniteJni.CACHE_EXIT_LOCK(target.Context, target.Target, id);
        }

        internal static bool CacheTryEnterLock(IUnmanagedTarget target, long id, long timeout)
        {
            return IgniteJni.CACHE_TRY_ENTER_LOCK(target.Context, target.Target, id, timeout);
        }

        internal static void CacheCloseLock(IUnmanagedTarget target, long id)
        {
            IgniteJni.CACHE_CLOSE_LOCK(target.Context, target.Target, id);
        }

        internal static void CacheRebalance(IUnmanagedTarget target, long futId)
        {
            IgniteJni.CACHE_REBALANCE(target.Context, target.Target, futId);
        }

        internal static void CacheStoreCallbackInvoke(IUnmanagedTarget target, long memPtr)
        {
            IgniteJni.CACHE_STORE_CALLBACK_INVOKE(target.Context, target.Target, memPtr);
        }

        internal static int CacheSize(IUnmanagedTarget target, int modes, bool loc)
        {
            return IgniteJni.CACHE_SIZE(target.Context, target.Target, modes, loc);
        }

        #endregion

        #region NATIVE METHODS: COMPUTE

        internal static void ComputeWithNoFailover(IUnmanagedTarget target)
        {
            IgniteJni.COMPUTE_WITH_NO_FAILOVER(target.Context, target.Target);
        }

        internal static void ComputeWithTimeout(IUnmanagedTarget target, long timeout)
        {
            IgniteJni.COMPUTE_WITH_TIMEOUT(target.Context, target.Target, timeout);
        }

        internal static void ComputeExecuteNative(IUnmanagedTarget target, long taskPtr, long topVer)
        {
            IgniteJni.COMPUTE_EXECUTE_NATIVE(target.Context, target.Target, taskPtr, topVer);
        }

        #endregion

        #region NATIVE METHODS: CONTINUOUS QUERY

        internal static void ContinuousQueryClose(IUnmanagedTarget target)
        {
            IgniteJni.ContinuousQryClose(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ContinuousQueryGetInitialQueryCursor(IUnmanagedTarget target)
        {
            void* res = IgniteJni.ContinuousQryGetInitialQueryCursor(target.Context, target.Target);

            return res == null ? null : target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: DATA STREAMER

        internal static void DataStreamerListenTopology(IUnmanagedTarget target, long ptr)
        {
            IgniteJni.DataStreamerListenTop(target.Context, target.Target, ptr);
        }

        internal static bool DataStreamerAllowOverwriteGet(IUnmanagedTarget target)
        {
            return IgniteJni.DATA_STREAMER_ALLOW_OVERWRITE_GET(target.Context, target.Target);
        }

        internal static void DataStreamerAllowOverwriteSet(IUnmanagedTarget target, bool val)
        {
            IgniteJni.DATA_STREAMER_ALLOW_OVERWRITE_SET(target.Context, target.Target, val);
        }

        internal static bool DataStreamerSkipStoreGet(IUnmanagedTarget target)
        {
            return IgniteJni.DATA_STREAMER_SKIP_STORE_GET(target.Context, target.Target);
        }

        internal static void DataStreamerSkipStoreSet(IUnmanagedTarget target, bool val)
        {
            IgniteJni.DATA_STREAMER_SKIP_STORE_SET(target.Context, target.Target, val);
        }

        internal static int DataStreamerPerNodeBufferSizeGet(IUnmanagedTarget target)
        {
            return IgniteJni.DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeBufferSizeSet(IUnmanagedTarget target, int val)
        {
            IgniteJni.DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET(target.Context, target.Target, val);
        }

        internal static int DataStreamerPerNodeParallelOperationsGet(IUnmanagedTarget target)
        {
            return IgniteJni.DataStreamerPerNodeParallelOpsGet(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeParallelOperationsSet(IUnmanagedTarget target, int val)
        {
            IgniteJni.DataStreamerPerNodeParallelOpsSet(target.Context, target.Target, val);
        }

        #endregion

        #region NATIVE METHODS: MESSAGING

        internal static IUnmanagedTarget MessagingWithASync(IUnmanagedTarget target)
        {
            void* res = IgniteJni.MessagingWithAsync(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: PROJECTION

        internal static IUnmanagedTarget ProjectionForOthers(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = IgniteJni.PROJECTION_FOR_OTHERS(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForRemotes(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROJECTION_FOR_REMOTES(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForDaemons(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROJECTION_FOR_DAEMONS(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForRandom(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROJECTION_FOR_RANDOM(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForOldest(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROJECTION_FOR_OLDEST(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForYoungest(IUnmanagedTarget target)
        {
            void* res = IgniteJni.PROJECTION_FOR_YOUNGEST(target.Context, target.Target);

            return target.ChangeTarget(res);
        }
        
        internal static void ProjectionResetMetrics(IUnmanagedTarget target)
        {
            IgniteJni.PROJECTION_RESET_METRICS(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ProjectionOutOpRet(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = IgniteJni.PROJECTION_OUT_OP_RET(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: QUERY CURSOR

        internal static void QueryCursorIterator(IUnmanagedTarget target)
        {
            IgniteJni.QryCursorIterator(target.Context, target.Target);
        }

        internal static void QueryCursorClose(IUnmanagedTarget target)
        {
            IgniteJni.QryCursorClose(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: TRANSACTIONS

        internal static long TransactionsStart(IUnmanagedTarget target, int concurrency, int isolation, long timeout, int txSize)
        {
            return IgniteJni.TxStart(target.Context, target.Target, concurrency, isolation, timeout, txSize);
        }

        internal static int TransactionsCommit(IUnmanagedTarget target, long id)
        {
            return IgniteJni.TxCommit(target.Context, target.Target, id);
        }

        internal static void TransactionsCommitAsync(IUnmanagedTarget target, long id, long futId)
        {
            IgniteJni.TxCommitAsync(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsRollback(IUnmanagedTarget target, long id)
        {
            return IgniteJni.TxRollback(target.Context, target.Target, id);
        }

        internal static void TransactionsRollbackAsync(IUnmanagedTarget target, long id, long futId)
        {
            IgniteJni.TxRollbackAsync(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsClose(IUnmanagedTarget target, long id)
        {
            return IgniteJni.TxClose(target.Context, target.Target, id);
        }

        internal static int TransactionsState(IUnmanagedTarget target, long id)
        {
            return IgniteJni.TxState(target.Context, target.Target, id);
        }

        internal static bool TransactionsSetRollbackOnly(IUnmanagedTarget target, long id)
        {
            return IgniteJni.TxSetRollbackOnly(target.Context, target.Target, id);
        }

        internal static void TransactionsResetMetrics(IUnmanagedTarget target)
        {
            IgniteJni.TxResetMetrics(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: MISCELANNEOUS

        internal static void Reallocate(long memPtr, int cap)
        {
            int res = IgniteJni.REALLOCATE(memPtr, cap);

            if (res != 0)
                throw new IgniteException("Failed to reallocate external memory [ptr=" + memPtr + 
                    ", capacity=" + cap + ']');
        }

        internal static IUnmanagedTarget Acquire(UnmanagedContext ctx, void* target)
        {
            void* target0 = IgniteJni.ACQUIRE(ctx.NativeContext, target);

            return new UnmanagedTarget(ctx, target0);
        }

        internal static void Release(IUnmanagedTarget target)
        {
            IgniteJni.RELEASE(target.Target);
        }

        internal static void ThrowToJava(void* ctx, Exception e)
        {
            char* msgChars = (char*)IgniteUtils.StringToUtf8Unmanaged(e.Message);

            try
            {
                IgniteJni.THROW_TO_JAVA(ctx, msgChars);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(msgChars));
            }
        }

        

        internal static int HandlersSize()
        {
            return IgniteJni.HANDLERS_SIZE();
        }

        internal static void* CreateContext(void* opts, int optsLen, void* cbs)
        {
            return IgniteJni.CREATE_CONTEXT(opts, optsLen, cbs);
        }

        internal static void DeleteContext(void* ctx)
        {
            IgniteJni.DELETE_CONTEXT(ctx);
        }

        internal static void DestroyJvm(void* ctx)
        {
            IgniteJni.DESTROY_JVM(ctx);
        }

        #endregion

        #region NATIVE METHODS: EVENTS

        internal static IUnmanagedTarget EventsWithAsync(IUnmanagedTarget target)
        {
            return target.ChangeTarget(IgniteJni.EVENTS_WITH_ASYNC(target.Context, target.Target));
        }

        internal static bool EventsStopLocalListen(IUnmanagedTarget target, long handle)
        {
            return IgniteJni.EVENTS_STOP_LOCAL_LISTEN(target.Context, target.Target, handle);
        }

        internal static bool EventsIsEnabled(IUnmanagedTarget target, int type)
        {
            return IgniteJni.EVENTS_IS_ENABLED(target.Context, target.Target, type);
        }

        internal static void EventsLocalListen(IUnmanagedTarget target, long handle, int type)
        {
            IgniteJni.EVENTS_LOCAL_LISTEN(target.Context, target.Target, handle, type);
        }

        #endregion

        #region NATIVE METHODS: SERVICES

        internal static IUnmanagedTarget ServicesWithAsync(IUnmanagedTarget target)
        {
            return target.ChangeTarget(IgniteJni.SERVICES_WITH_ASYNC(target.Context, target.Target));
        }

        internal static IUnmanagedTarget ServicesWithServerKeepBinary(IUnmanagedTarget target)
        {
            return target.ChangeTarget(IgniteJni.SERVICES_WITH_SERVER_KEEP_BINARY(target.Context, target.Target));
        }

        internal static void ServicesCancel(IUnmanagedTarget target, string name)
        {
            var nameChars = (char*)IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                IgniteJni.SERVICES_CANCEL(target.Context, target.Target, nameChars);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(nameChars));
            }
        }

        internal static void ServicesCancelAll(IUnmanagedTarget target)
        {
            IgniteJni.SERVICES_CANCEL_ALL(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ServicesGetServiceProxy(IUnmanagedTarget target, string name, bool sticky)
        {
            var nameChars = (char*)IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                return target.ChangeTarget(IgniteJni.SERVICES_GET_SERVICE_PROXY(target.Context, target.Target, nameChars, sticky));
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
            return IgniteJni.ATOMIC_LONG_GET(target.Context, target.Target);
        }

        internal static long AtomicLongIncrementAndGet(IUnmanagedTarget target)
        {
            return IgniteJni.ATOMIC_LONG_INCREMENT_AND_GET(target.Context, target.Target);
        }

        internal static long AtomicLongAddAndGet(IUnmanagedTarget target, long value)
        {
            return IgniteJni.ATOMIC_LONG_ADD_AND_GET(target.Context, target.Target, value);
        }

        internal static long AtomicLongDecrementAndGet(IUnmanagedTarget target)
        {
            return IgniteJni.ATOMIC_LONG_DECREMENT_AND_GET(target.Context, target.Target);
        }

        internal static long AtomicLongGetAndSet(IUnmanagedTarget target, long value)
        {
            return IgniteJni.ATOMIC_LONG_GET_AND_SET(target.Context, target.Target, value);
        }

        internal static long AtomicLongCompareAndSetAndGet(IUnmanagedTarget target, long expVal, long newVal)
        {
            return IgniteJni.ATOMIC_LONG_COMPARE_AND_SET_AND_GET(target.Context, target.Target, expVal, newVal);
        }

        internal static bool AtomicLongIsClosed(IUnmanagedTarget target)
        {
            return IgniteJni.ATOMIC_LONG_IS_CLOSED(target.Context, target.Target);
        }

        internal static void AtomicLongClose(IUnmanagedTarget target)
        {
            IgniteJni.ATOMIC_LONG_CLOSE(target.Context, target.Target);
        }

        #endregion
    }
}
