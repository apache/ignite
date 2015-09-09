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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Unmanaged utility classes.
    /// </summary>
    internal static unsafe class UnmanagedUtils
    {
        /** Interop factory ID for .Net. */
        private const int InteropFactoryId = 1;

        #region PROCEDURE NAMES

        private const string ProcReallocate = "IgniteReallocate";

        private const string ProcIgnitionStart = "IgniteIgnitionStart";
        private const string ProcIgnitionStop = "IgniteIgnitionStop";
        private const string ProcIgnitionStopAll = "IgniteIgnitionStopAll";
        
        private const string ProcProcessorReleaseStart = "IgniteProcessorReleaseStart";
        private const string ProcProcessorProjection = "IgniteProcessorProjection";
        private const string ProcProcessorCache = "IgniteProcessorCache";
        private const string ProcProcessorGetOrCreateCache = "IgniteProcessorGetOrCreateCache";
        private const string ProcProcessorCreateCache = "IgniteProcessorCreateCache";
        private const string ProcProcessorAffinity = "IgniteProcessorAffinity";
        private const string ProcProcessorDataStreamer = "IgniteProcessorDataStreamer";
        private const string ProcProcessorTransactions = "IgniteProcessorTransactions";
        private const string ProcProcessorCompute = "IgniteProcessorCompute";
        private const string ProcProcessorMessage = "IgniteProcessorMessage";
        private const string ProcProcessorEvents = "IgniteProcessorEvents";
        private const string ProcProcessorServices = "IgniteProcessorServices";
        private const string ProcProcessorExtensions = "IgniteProcessorExtensions";
        
        private const string ProcTargetInStreamOutLong = "IgniteTargetInStreamOutLong";
        private const string ProcTargetInStreamOutStream = "IgniteTargetInStreamOutStream";
        private const string ProcTargetInStreamOutObject = "IgniteTargetInStreamOutObject";
        private const string ProcTargetInObjectStreamOutStream = "IgniteTargetInObjectStreamOutStream";
        private const string ProcTargetOutLong = "IgniteTargetOutLong";
        private const string ProcTargetOutStream = "IgniteTargetOutStream";
        private const string ProcTargetOutObject = "IgniteTargetOutObject";
        private const string ProcTargetListenFut = "IgniteTargetListenFuture";
        private const string ProcTargetListenFutForOp = "IgniteTargetListenFutureForOperation";

        private const string ProcAffinityParts = "IgniteAffinityPartitions";

        private const string ProcCacheWithSkipStore = "IgniteCacheWithSkipStore";
        private const string ProcCacheWithNoRetries = "IgniteCacheWithNoRetries";
        private const string ProcCacheWithExpiryPolicy = "IgniteCacheWithExpiryPolicy";
        private const string ProcCacheWithAsync = "IgniteCacheWithAsync";
        private const string ProcCacheWithKeepPortable = "IgniteCacheWithKeepPortable";
        private const string ProcCacheClear = "IgniteCacheClear";
        private const string ProcCacheRemoveAll = "IgniteCacheRemoveAll";
        private const string ProcCacheOutOpQueryCursor = "IgniteCacheOutOpQueryCursor";
        private const string ProcCacheOutOpContinuousQuery = "IgniteCacheOutOpContinuousQuery";
        private const string ProcCacheIterator = "IgniteCacheIterator";
        private const string ProcCacheLocalIterator = "IgniteCacheLocalIterator";
        private const string ProcCacheEnterLock = "IgniteCacheEnterLock";
        private const string ProcCacheExitLock = "IgniteCacheExitLock";
        private const string ProcCacheTryEnterLock = "IgniteCacheTryEnterLock";
        private const string ProcCacheCloseLock = "IgniteCacheCloseLock";
        private const string ProcCacheRebalance = "IgniteCacheRebalance";
        private const string ProcCacheSize = "IgniteCacheSize";

        private const string ProcCacheStoreCallbackInvoke = "IgniteCacheStoreCallbackInvoke";

        private const string ProcComputeWithNoFailover = "IgniteComputeWithNoFailover";
        private const string ProcComputeWithTimeout = "IgniteComputeWithTimeout";
        private const string ProcComputeExecuteNative = "IgniteComputeExecuteNative";

        private const string ProcContinuousQryClose = "IgniteContinuousQueryClose";
        private const string ProcContinuousQryGetInitialQueryCursor = "IgniteContinuousQueryGetInitialQueryCursor";

        private const string ProcDataStreamerListenTop = "IgniteDataStreamerListenTopology";
        private const string ProcDataStreamerAllowOverwriteGet = "IgniteDataStreamerAllowOverwriteGet";
        private const string ProcDataStreamerAllowOverwriteSet = "IgniteDataStreamerAllowOverwriteSet";
        private const string ProcDataStreamerSkipStoreGet = "IgniteDataStreamerSkipStoreGet";
        private const string ProcDataStreamerSkipStoreSet = "IgniteDataStreamerSkipStoreSet";
        private const string ProcDataStreamerPerNodeBufferSizeGet = "IgniteDataStreamerPerNodeBufferSizeGet";
        private const string ProcDataStreamerPerNodeBufferSizeSet = "IgniteDataStreamerPerNodeBufferSizeSet";
        private const string ProcDataStreamerPerNodeParallelOpsGet = "IgniteDataStreamerPerNodeParallelOperationsGet";
        private const string ProcDataStreamerPerNodeParallelOpsSet = "IgniteDataStreamerPerNodeParallelOperationsSet";

        private const string ProcMessagingWithAsync = "IgniteMessagingWithAsync";

        private const string ProcQryCursorIterator = "IgniteQueryCursorIterator";
        private const string ProcQryCursorClose = "IgniteQueryCursorClose";

        private const string ProcProjectionForOthers = "IgniteProjectionForOthers";
        private const string ProcProjectionForRemotes = "IgniteProjectionForRemotes";
        private const string ProcProjectionForDaemons = "IgniteProjectionForDaemons";
        private const string ProcProjectionForRandom = "IgniteProjectionForRandom";
        private const string ProcProjectionForOldest = "IgniteProjectionForOldest";
        private const string ProcProjectionForYoungest = "IgniteProjectionForYoungest";
        private const string ProcProjectionResetMetrics = "IgniteProjectionResetMetrics";
        private const string ProcProjectionOutOpRet = "IgniteProjectionOutOpRet";

        private const string ProcRelease = "IgniteRelease";

        private const string ProcTxStart = "IgniteTransactionsStart";
        private const string ProcTxCommit = "IgniteTransactionsCommit";
        private const string ProcTxCommitAsync = "IgniteTransactionsCommitAsync";
        private const string ProcTxRollback = "IgniteTransactionsRollback";
        private const string ProcTxRollbackAsync = "IgniteTransactionsRollbackAsync";
        private const string ProcTxClose = "IgniteTransactionsClose";
        private const string ProcTxState = "IgniteTransactionsState";
        private const string ProcTxSetRollbackOnly = "IgniteTransactionsSetRollbackOnly";
        private const string ProcTxResetMetrics = "IgniteTransactionsResetMetrics";

        private const string ProcThrowToJava = "IgniteThrowToJava";

        private const string ProcDestroyJvm = "IgniteDestroyJvm";

        private const string ProcHandlersSize = "IgniteHandlersSize";

        private const string ProcCreateContext = "IgniteCreateContext";
        
        private const string ProcEventsWithAsync = "IgniteEventsWithAsync";
        private const string ProcEventsStopLocalListen = "IgniteEventsStopLocalListen";
        private const string ProcEventsLocalListen = "IgniteEventsLocalListen";
        private const string ProcEventsIsEnabled = "IgniteEventsIsEnabled";

        private const string ProcDeleteContext = "IgniteDeleteContext";
        
        private const string ProcServicesWithAsync = "IgniteServicesWithAsync";
        private const string ProcServicesWithServerKeepPortable = "IgniteServicesWithServerKeepPortable";
        private const string ProcServicesCancel = "IgniteServicesCancel";
        private const string ProcServicesCancelAll = "IgniteServicesCancelAll";
        private const string ProcServicesGetServiceProxy = "IgniteServicesGetServiceProxy";
        
        #endregion

        #region DELEGATE DEFINITIONS

        private delegate int ReallocateDelegate(long memPtr, int cap);

        private delegate void* IgnitionStartDelegate(void* ctx, sbyte* cfgPath, sbyte* gridName, int factoryId, long dataPtr);
        private delegate bool IgnitionStopDelegate(void* ctx, sbyte* gridName, bool cancel);
        private delegate void IgnitionStopAllDelegate(void* ctx, bool cancel);

        private delegate void ProcessorReleaseStartDelegate(void* ctx, void* obj);
        private delegate void* ProcessorProjectionDelegate(void* ctx, void* obj);
        private delegate void* ProcessorCacheDelegate(void* ctx, void* obj, sbyte* name);
        private delegate void* ProcessorCreateCacheDelegate(void* ctx, void* obj, sbyte* name);
        private delegate void* ProcessorGetOrCreateCacheDelegate(void* ctx, void* obj, sbyte* name);
        private delegate void* ProcessorAffinityDelegate(void* ctx, void* obj, sbyte* name);
        private delegate void* ProcessorDataStreamerDelegate(void* ctx, void* obj, sbyte* name, bool keepPortable);
        private delegate void* ProcessorTransactionsDelegate(void* ctx, void* obj);
        private delegate void* ProcessorComputeDelegate(void* ctx, void* obj, void* prj);
        private delegate void* ProcessorMessageDelegate(void* ctx, void* obj, void* prj);
        private delegate void* ProcessorEventsDelegate(void* ctx, void* obj, void* prj);
        private delegate void* ProcessorServicesDelegate(void* ctx, void* obj, void* prj);
        private delegate void* ProcessorExtensionsDelegate(void* ctx, void* obj);
        
        private delegate long TargetInStreamOutLongDelegate(void* ctx, void* target, int opType, long memPtr);
        private delegate void TargetInStreamOutStreamDelegate(void* ctx, void* target, int opType, long inMemPtr, long outMemPtr);
        private delegate void* TargetInStreamOutObjectDelegate(void* ctx, void* target, int opType, long memPtr);
        private delegate void TargetInObjectStreamOutStreamDelegate(void* ctx, void* target, int opType, void* arg, long inMemPtr, long outMemPtr);
        private delegate long TargetOutLongDelegate(void* ctx, void* target, int opType);
        private delegate void TargetOutStreamDelegate(void* ctx, void* target, int opType, long memPtr);
        private delegate void* TargetOutObjectDelegate(void* ctx, void* target, int opType);
        private delegate void TargetListenFutureDelegate(void* ctx, void* target, long futId, int typ);
        private delegate void TargetListenFutureForOpDelegate(void* ctx, void* target, long futId, int typ, int opId);

        private delegate int AffinityPartitionsDelegate(void* ctx, void* target);

        private delegate void* CacheWithSkipStoreDelegate(void* ctx, void* obj);
        private delegate void* CacheNoRetriesDelegate(void* ctx, void* obj);
        private delegate void* CacheWithExpiryPolicyDelegate(void* ctx, void* obj, long create, long update, long access);
        private delegate void* CacheWithAsyncDelegate(void* ctx, void* obj);
        private delegate void* CacheWithKeepPortableDelegate(void* ctx, void* obj);
        private delegate void CacheClearDelegate(void* ctx, void* obj);
        private delegate void CacheRemoveAllDelegate(void* ctx, void* obj);
        private delegate void* CacheOutOpQueryCursorDelegate(void* ctx, void* obj, int type, long memPtr);
        private delegate void* CacheOutOpContinuousQueryDelegate(void* ctx, void* obj, int type, long memPtr);
        private delegate void* CacheIteratorDelegate(void* ctx, void* obj);
        private delegate void* CacheLocalIteratorDelegate(void* ctx, void* obj, int peekModes);
        private delegate void CacheEnterLockDelegate(void* ctx, void* obj, long id);
        private delegate void CacheExitLockDelegate(void* ctx, void* obj, long id);
        private delegate bool CacheTryEnterLockDelegate(void* ctx, void* obj, long id, long timeout);
        private delegate void CacheCloseLockDelegate(void* ctx, void* obj, long id);
        private delegate void CacheRebalanceDelegate(void* ctx, void* obj, long futId);
        private delegate int CacheSizeDelegate(void* ctx, void* obj, int peekModes, bool loc);

        private delegate void CacheStoreCallbackInvokeDelegate(void* ctx, void* obj, long memPtr);

        private delegate void ComputeWithNoFailoverDelegate(void* ctx, void* target);
        private delegate void ComputeWithTimeoutDelegate(void* ctx, void* target, long timeout);
        private delegate void ComputeExecuteNativeDelegate(void* ctx, void* target, long taskPtr, long topVer);

        private delegate void ContinuousQueryCloseDelegate(void* ctx, void* target);
        private delegate void* ContinuousQueryGetInitialQueryCursorDelegate(void* ctx, void* target);

        private delegate void DataStreamerListenTopologyDelegate(void* ctx, void* obj, long ptr);
        private delegate bool DataStreamerAllowOverwriteGetDelegate(void* ctx, void* obj);
        private delegate void DataStreamerAllowOverwriteSetDelegate(void* ctx, void* obj, bool val);
        private delegate bool DataStreamerSkipStoreGetDelegate(void* ctx, void* obj);
        private delegate void DataStreamerSkipStoreSetDelegate(void* ctx, void* obj, bool val);
        private delegate int DataStreamerPerNodeBufferSizeGetDelegate(void* ctx, void* obj);
        private delegate void DataStreamerPerNodeBufferSizeSetDelegate(void* ctx, void* obj, int val);
        private delegate int DataStreamerPerNodeParallelOperationsGetDelegate(void* ctx, void* obj);
        private delegate void DataStreamerPerNodeParallelOperationsSetDelegate(void* ctx, void* obj, int val);

        private delegate void* MessagingWithAsyncDelegate(void* ctx, void* target);

        private delegate void* ProjectionForOthersDelegate(void* ctx, void* obj, void* prj);
		private delegate void* ProjectionForRemotesDelegate(void* ctx, void* obj);
		private delegate void* ProjectionForDaemonsDelegate(void* ctx, void* obj);
		private delegate void* ProjectionForRandomDelegate(void* ctx, void* obj);
		private delegate void* ProjectionForOldestDelegate(void* ctx, void* obj);
		private delegate void* ProjectionForYoungestDelegate(void* ctx, void* obj);
		private delegate void ProjectionResetMetricsDelegate(void* ctx, void* obj);
		private delegate void* ProjectionOutOpRetDelegate(void* ctx, void* obj, int type, long memPtr);

        private delegate void QueryCursorIteratorDelegate(void* ctx, void* target);
        private delegate void QueryCursorCloseDelegate(void* ctx, void* target);

        private delegate void ReleaseDelegate(void* target);

        private delegate long TransactionsStartDelegate(void* ctx, void* target, int concurrency, int isolation, long timeout, int txSize);
        private delegate int TransactionsCommitDelegate(void* ctx, void* target, long id);
        private delegate void TransactionsCommitAsyncDelegate(void* ctx, void* target, long id, long futId);
        private delegate int TransactionsRollbackDelegate(void* ctx, void* target, long id);
        private delegate void TransactionsRollbackAsyncDelegate(void* ctx, void* target, long id, long futId);
        private delegate int TransactionsCloseDelegate(void* ctx, void* target, long id);
        private delegate int TransactionsStateDelegate(void* ctx, void* target, long id);
        private delegate bool TransactionsSetRollbackOnlyDelegate(void* ctx, void* target, long id);
        private delegate void TransactionsResetMetricsDelegate(void* ctx, void* target);

        private delegate void ThrowToJavaDelegate(void* ctx, char* msg);

        private delegate void DestroyJvmDelegate(void* ctx);

        private delegate int HandlersSizeDelegate();

        private delegate void* CreateContextDelegate(void* opts, int optsLen, void* cbs);
        
        private delegate void* EventsWithAsyncDelegate(void* ctx, void* obj);
        private delegate bool EventsStopLocalListenDelegate(void* ctx, void* obj, long hnd);
        private delegate void EventsLocalListenDelegate(void* ctx, void* obj, long hnd, int type);
        private delegate bool EventsIsEnabledDelegate(void* ctx, void* obj, int type);

        private delegate void DeleteContextDelegate(void* ptr);

        private delegate void* ServicesWithAsyncDelegate(void* ctx, void* target);
        private delegate void* ServicesWithServerKeepPortableDelegate(void* ctx, void* target);
        private delegate long ServicesCancelDelegate(void* ctx, void* target, char* name);
        private delegate long ServicesCancelAllDelegate(void* ctx, void* target);
        private delegate void* ServicesGetServiceProxyDelegate(void* ctx, void* target, char* name, bool sticky);

        #endregion

        #region DELEGATE MEMBERS

        // ReSharper disable InconsistentNaming
        private static readonly ReallocateDelegate REALLOCATE;

        private static readonly IgnitionStartDelegate IGNITION_START;
        private static readonly IgnitionStopDelegate IGNITION_STOP;
        private static readonly IgnitionStopAllDelegate IGNITION_STOP_ALL;

        private static readonly ProcessorReleaseStartDelegate PROCESSOR_RELEASE_START;
        private static readonly ProcessorProjectionDelegate PROCESSOR_PROJECTION;
        private static readonly ProcessorCacheDelegate PROCESSOR_CACHE;
        private static readonly ProcessorCreateCacheDelegate PROCESSOR_CREATE_CACHE;
        private static readonly ProcessorGetOrCreateCacheDelegate PROCESSOR_GET_OR_CREATE_CACHE;
        private static readonly ProcessorAffinityDelegate PROCESSOR_AFFINITY;
        private static readonly ProcessorDataStreamerDelegate PROCESSOR_DATA_STREAMER;
        private static readonly ProcessorTransactionsDelegate PROCESSOR_TRANSACTIONS;
        private static readonly ProcessorComputeDelegate PROCESSOR_COMPUTE;
        private static readonly ProcessorMessageDelegate PROCESSOR_MESSAGE;
        private static readonly ProcessorEventsDelegate PROCESSOR_EVENTS;
        private static readonly ProcessorServicesDelegate PROCESSOR_SERVICES;
        private static readonly ProcessorExtensionsDelegate PROCESSOR_EXTENSIONS;
        
        private static readonly TargetInStreamOutLongDelegate TARGET_IN_STREAM_OUT_LONG;
        private static readonly TargetInStreamOutStreamDelegate TARGET_IN_STREAM_OUT_STREAM;
        private static readonly TargetInStreamOutObjectDelegate TARGET_IN_STREAM_OUT_OBJECT;
        private static readonly TargetInObjectStreamOutStreamDelegate TARGET_IN_OBJECT_STREAM_OUT_STREAM;
        private static readonly TargetOutLongDelegate TARGET_OUT_LONG;
        private static readonly TargetOutStreamDelegate TARGET_OUT_STREAM;
        private static readonly TargetOutObjectDelegate TARGET_OUT_OBJECT;
        private static readonly TargetListenFutureDelegate TargetListenFut;
        private static readonly TargetListenFutureForOpDelegate TargetListenFutForOp;

        private static readonly AffinityPartitionsDelegate AffinityParts;

        private static readonly CacheWithSkipStoreDelegate CACHE_WITH_SKIP_STORE;
        private static readonly CacheNoRetriesDelegate CACHE_WITH_NO_RETRIES;
        private static readonly CacheWithExpiryPolicyDelegate CACHE_WITH_EXPIRY_POLICY;
        private static readonly CacheWithAsyncDelegate CACHE_WITH_ASYNC;
        private static readonly CacheWithKeepPortableDelegate CACHE_WITH_KEEP_PORTABLE;
        private static readonly CacheClearDelegate CACHE_CLEAR;
        private static readonly CacheRemoveAllDelegate CACHE_REMOVE_ALL;
        private static readonly CacheOutOpQueryCursorDelegate CACHE_OUT_OP_QUERY_CURSOR;
        private static readonly CacheOutOpContinuousQueryDelegate CACHE_OUT_OP_CONTINUOUS_QUERY;
        private static readonly CacheIteratorDelegate CACHE_ITERATOR;
        private static readonly CacheLocalIteratorDelegate CACHE_LOCAL_ITERATOR;
        private static readonly CacheEnterLockDelegate CACHE_ENTER_LOCK;
        private static readonly CacheExitLockDelegate CACHE_EXIT_LOCK;
        private static readonly CacheTryEnterLockDelegate CACHE_TRY_ENTER_LOCK;
        private static readonly CacheCloseLockDelegate CACHE_CLOSE_LOCK;
        private static readonly CacheRebalanceDelegate CACHE_REBALANCE;
        private static readonly CacheSizeDelegate CACHE_SIZE;

        private static readonly CacheStoreCallbackInvokeDelegate CACHE_STORE_CALLBACK_INVOKE;

        private static readonly ComputeWithNoFailoverDelegate COMPUTE_WITH_NO_FAILOVER;
        private static readonly ComputeWithTimeoutDelegate COMPUTE_WITH_TIMEOUT;
        private static readonly ComputeExecuteNativeDelegate COMPUTE_EXECUTE_NATIVE;

        private static readonly ContinuousQueryCloseDelegate ContinuousQryClose;
        private static readonly ContinuousQueryGetInitialQueryCursorDelegate ContinuousQryGetInitialQueryCursor;

        private static readonly DataStreamerListenTopologyDelegate DataStreamerListenTop;
        private static readonly DataStreamerAllowOverwriteGetDelegate DATA_STREAMER_ALLOW_OVERWRITE_GET;
        private static readonly DataStreamerAllowOverwriteSetDelegate DATA_STREAMER_ALLOW_OVERWRITE_SET;
        private static readonly DataStreamerSkipStoreGetDelegate DATA_STREAMER_SKIP_STORE_GET;
        private static readonly DataStreamerSkipStoreSetDelegate DATA_STREAMER_SKIP_STORE_SET;
        private static readonly DataStreamerPerNodeBufferSizeGetDelegate DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET;
        private static readonly DataStreamerPerNodeBufferSizeSetDelegate DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET;
        private static readonly DataStreamerPerNodeParallelOperationsGetDelegate DataStreamerPerNodeParallelOpsGet;
        private static readonly DataStreamerPerNodeParallelOperationsSetDelegate DataStreamerPerNodeParallelOpsSet;

        private static readonly MessagingWithAsyncDelegate MessagingWithAsync;

        private static readonly ProjectionForOthersDelegate PROJECTION_FOR_OTHERS;
        private static readonly ProjectionForRemotesDelegate PROJECTION_FOR_REMOTES;
        private static readonly ProjectionForDaemonsDelegate PROJECTION_FOR_DAEMONS;
        private static readonly ProjectionForRandomDelegate PROJECTION_FOR_RANDOM;
        private static readonly ProjectionForOldestDelegate PROJECTION_FOR_OLDEST;
        private static readonly ProjectionForYoungestDelegate PROJECTION_FOR_YOUNGEST;
        private static readonly ProjectionResetMetricsDelegate PROJECTION_RESET_METRICS;
        private static readonly ProjectionOutOpRetDelegate PROJECTION_OUT_OP_RET;

        private static readonly QueryCursorIteratorDelegate QryCursorIterator;
        private static readonly QueryCursorCloseDelegate QryCursorClose;

        private static readonly ReleaseDelegate RELEASE;

        private static readonly TransactionsStartDelegate TxStart;
        private static readonly TransactionsCommitDelegate TxCommit;
        private static readonly TransactionsCommitAsyncDelegate TxCommitAsync;
        private static readonly TransactionsRollbackDelegate TxRollback;
        private static readonly TransactionsRollbackAsyncDelegate TxRollbackAsync;
        private static readonly TransactionsCloseDelegate TxClose;
        private static readonly TransactionsStateDelegate TxState;
        private static readonly TransactionsSetRollbackOnlyDelegate TxSetRollbackOnly;
        private static readonly TransactionsResetMetricsDelegate TxResetMetrics;

        private static readonly ThrowToJavaDelegate THROW_TO_JAVA;

        private static readonly DestroyJvmDelegate DESTROY_JVM;

        private static readonly HandlersSizeDelegate HANDLERS_SIZE;

        private static readonly CreateContextDelegate CREATE_CONTEXT;
        
        private static readonly EventsWithAsyncDelegate EVENTS_WITH_ASYNC;
        private static readonly EventsStopLocalListenDelegate EVENTS_STOP_LOCAL_LISTEN;
        private static readonly EventsLocalListenDelegate EVENTS_LOCAL_LISTEN;
        private static readonly EventsIsEnabledDelegate EVENTS_IS_ENABLED;
 
        private static readonly DeleteContextDelegate DELETE_CONTEXT;
        
        private static readonly ServicesWithAsyncDelegate SERVICES_WITH_ASYNC;
        private static readonly ServicesWithServerKeepPortableDelegate SERVICES_WITH_SERVER_KEEP_PORTABLE;
        private static readonly ServicesCancelDelegate SERVICES_CANCEL;
        private static readonly ServicesCancelAllDelegate SERVICES_CANCEL_ALL;
        private static readonly ServicesGetServiceProxyDelegate SERVICES_GET_SERVICE_PROXY;
        // ReSharper restore InconsistentNaming

        #endregion

        /** Library pointer. */
        private static readonly IntPtr Ptr;

        /// <summary>
        /// Initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        static UnmanagedUtils()
        {
            var path = IgniteUtils.UnpackEmbeddedResource(IgniteUtils.FileIgniteJniDll);

            Ptr = NativeMethods.LoadLibrary(path);

            if (Ptr == IntPtr.Zero)
                throw new IgniteException("Failed to load " + IgniteUtils.FileIgniteJniDll + ": " + Marshal.GetLastWin32Error());

            REALLOCATE = CreateDelegate<ReallocateDelegate>(ProcReallocate);

            IGNITION_START = CreateDelegate<IgnitionStartDelegate>(ProcIgnitionStart);
            IGNITION_STOP = CreateDelegate<IgnitionStopDelegate>(ProcIgnitionStop);
            IGNITION_STOP_ALL = CreateDelegate<IgnitionStopAllDelegate>(ProcIgnitionStopAll);
            
            PROCESSOR_RELEASE_START = CreateDelegate<ProcessorReleaseStartDelegate>(ProcProcessorReleaseStart);
            PROCESSOR_PROJECTION = CreateDelegate<ProcessorProjectionDelegate>(ProcProcessorProjection);
            PROCESSOR_CACHE = CreateDelegate<ProcessorCacheDelegate>(ProcProcessorCache);
            PROCESSOR_CREATE_CACHE = CreateDelegate<ProcessorCreateCacheDelegate>(ProcProcessorCreateCache);
            PROCESSOR_GET_OR_CREATE_CACHE = CreateDelegate<ProcessorGetOrCreateCacheDelegate>(ProcProcessorGetOrCreateCache);
            PROCESSOR_AFFINITY = CreateDelegate<ProcessorAffinityDelegate>(ProcProcessorAffinity);
            PROCESSOR_DATA_STREAMER = CreateDelegate<ProcessorDataStreamerDelegate>(ProcProcessorDataStreamer);
            PROCESSOR_TRANSACTIONS = CreateDelegate<ProcessorTransactionsDelegate>(ProcProcessorTransactions);
            PROCESSOR_COMPUTE = CreateDelegate<ProcessorComputeDelegate>(ProcProcessorCompute);
            PROCESSOR_MESSAGE = CreateDelegate<ProcessorMessageDelegate>(ProcProcessorMessage);
            PROCESSOR_EVENTS = CreateDelegate<ProcessorEventsDelegate>(ProcProcessorEvents);
            PROCESSOR_SERVICES = CreateDelegate<ProcessorServicesDelegate>(ProcProcessorServices);
            PROCESSOR_EXTENSIONS = CreateDelegate<ProcessorExtensionsDelegate>(ProcProcessorExtensions);
            
            TARGET_IN_STREAM_OUT_LONG = CreateDelegate<TargetInStreamOutLongDelegate>(ProcTargetInStreamOutLong);
            TARGET_IN_STREAM_OUT_STREAM = CreateDelegate<TargetInStreamOutStreamDelegate>(ProcTargetInStreamOutStream);
            TARGET_IN_STREAM_OUT_OBJECT = CreateDelegate<TargetInStreamOutObjectDelegate>(ProcTargetInStreamOutObject);
            TARGET_IN_OBJECT_STREAM_OUT_STREAM = CreateDelegate<TargetInObjectStreamOutStreamDelegate>(ProcTargetInObjectStreamOutStream);
            TARGET_OUT_LONG = CreateDelegate<TargetOutLongDelegate>(ProcTargetOutLong);
            TARGET_OUT_STREAM = CreateDelegate<TargetOutStreamDelegate>(ProcTargetOutStream);
            TARGET_OUT_OBJECT = CreateDelegate<TargetOutObjectDelegate>(ProcTargetOutObject);
            TargetListenFut = CreateDelegate<TargetListenFutureDelegate>(ProcTargetListenFut);
            TargetListenFutForOp = CreateDelegate<TargetListenFutureForOpDelegate>(ProcTargetListenFutForOp);

            AffinityParts = CreateDelegate<AffinityPartitionsDelegate>(ProcAffinityParts);

            CACHE_WITH_SKIP_STORE = CreateDelegate<CacheWithSkipStoreDelegate>(ProcCacheWithSkipStore);
            CACHE_WITH_NO_RETRIES = CreateDelegate<CacheNoRetriesDelegate>(ProcCacheWithNoRetries);
            CACHE_WITH_EXPIRY_POLICY = CreateDelegate<CacheWithExpiryPolicyDelegate>(ProcCacheWithExpiryPolicy);
            CACHE_WITH_ASYNC = CreateDelegate<CacheWithAsyncDelegate>(ProcCacheWithAsync);
            CACHE_WITH_KEEP_PORTABLE = CreateDelegate<CacheWithKeepPortableDelegate>(ProcCacheWithKeepPortable);
            CACHE_CLEAR = CreateDelegate<CacheClearDelegate>(ProcCacheClear);
            CACHE_REMOVE_ALL = CreateDelegate<CacheRemoveAllDelegate>(ProcCacheRemoveAll);
            CACHE_OUT_OP_QUERY_CURSOR = CreateDelegate<CacheOutOpQueryCursorDelegate>(ProcCacheOutOpQueryCursor);
            CACHE_OUT_OP_CONTINUOUS_QUERY = CreateDelegate<CacheOutOpContinuousQueryDelegate>(ProcCacheOutOpContinuousQuery);
            CACHE_ITERATOR = CreateDelegate<CacheIteratorDelegate>(ProcCacheIterator);
            CACHE_LOCAL_ITERATOR = CreateDelegate<CacheLocalIteratorDelegate>(ProcCacheLocalIterator);
            CACHE_ENTER_LOCK = CreateDelegate<CacheEnterLockDelegate>(ProcCacheEnterLock);
            CACHE_EXIT_LOCK = CreateDelegate<CacheExitLockDelegate>(ProcCacheExitLock);
            CACHE_TRY_ENTER_LOCK = CreateDelegate<CacheTryEnterLockDelegate>(ProcCacheTryEnterLock);
            CACHE_CLOSE_LOCK = CreateDelegate<CacheCloseLockDelegate>(ProcCacheCloseLock);
            CACHE_REBALANCE = CreateDelegate<CacheRebalanceDelegate>(ProcCacheRebalance);
            CACHE_SIZE = CreateDelegate<CacheSizeDelegate>(ProcCacheSize);

            CACHE_STORE_CALLBACK_INVOKE = CreateDelegate<CacheStoreCallbackInvokeDelegate>(ProcCacheStoreCallbackInvoke);

            COMPUTE_WITH_NO_FAILOVER = CreateDelegate<ComputeWithNoFailoverDelegate>(ProcComputeWithNoFailover);
            COMPUTE_WITH_TIMEOUT = CreateDelegate<ComputeWithTimeoutDelegate>(ProcComputeWithTimeout);
            COMPUTE_EXECUTE_NATIVE = CreateDelegate<ComputeExecuteNativeDelegate>(ProcComputeExecuteNative);

            ContinuousQryClose = CreateDelegate<ContinuousQueryCloseDelegate>(ProcContinuousQryClose);
            ContinuousQryGetInitialQueryCursor = CreateDelegate<ContinuousQueryGetInitialQueryCursorDelegate>(ProcContinuousQryGetInitialQueryCursor);

            DataStreamerListenTop = CreateDelegate<DataStreamerListenTopologyDelegate>(ProcDataStreamerListenTop); 
            DATA_STREAMER_ALLOW_OVERWRITE_GET = CreateDelegate<DataStreamerAllowOverwriteGetDelegate>(ProcDataStreamerAllowOverwriteGet);
            DATA_STREAMER_ALLOW_OVERWRITE_SET = CreateDelegate<DataStreamerAllowOverwriteSetDelegate>(ProcDataStreamerAllowOverwriteSet); 
            DATA_STREAMER_SKIP_STORE_GET = CreateDelegate<DataStreamerSkipStoreGetDelegate>(ProcDataStreamerSkipStoreGet); 
            DATA_STREAMER_SKIP_STORE_SET = CreateDelegate<DataStreamerSkipStoreSetDelegate>(ProcDataStreamerSkipStoreSet); 
            DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET = CreateDelegate<DataStreamerPerNodeBufferSizeGetDelegate>(ProcDataStreamerPerNodeBufferSizeGet); 
            DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET = CreateDelegate<DataStreamerPerNodeBufferSizeSetDelegate>(ProcDataStreamerPerNodeBufferSizeSet); 
            DataStreamerPerNodeParallelOpsGet = CreateDelegate<DataStreamerPerNodeParallelOperationsGetDelegate>(ProcDataStreamerPerNodeParallelOpsGet); 
            DataStreamerPerNodeParallelOpsSet = CreateDelegate<DataStreamerPerNodeParallelOperationsSetDelegate>(ProcDataStreamerPerNodeParallelOpsSet); 

            MessagingWithAsync = CreateDelegate<MessagingWithAsyncDelegate>(ProcMessagingWithAsync);

            PROJECTION_FOR_OTHERS = CreateDelegate<ProjectionForOthersDelegate>(ProcProjectionForOthers);
            PROJECTION_FOR_REMOTES = CreateDelegate<ProjectionForRemotesDelegate>(ProcProjectionForRemotes);
            PROJECTION_FOR_DAEMONS = CreateDelegate<ProjectionForDaemonsDelegate>(ProcProjectionForDaemons);
            PROJECTION_FOR_RANDOM = CreateDelegate<ProjectionForRandomDelegate>(ProcProjectionForRandom);
            PROJECTION_FOR_OLDEST = CreateDelegate<ProjectionForOldestDelegate>(ProcProjectionForOldest);
            PROJECTION_FOR_YOUNGEST = CreateDelegate<ProjectionForYoungestDelegate>(ProcProjectionForYoungest);
            PROJECTION_RESET_METRICS = CreateDelegate<ProjectionResetMetricsDelegate>(ProcProjectionResetMetrics);
            PROJECTION_OUT_OP_RET = CreateDelegate<ProjectionOutOpRetDelegate>(ProcProjectionOutOpRet);

            QryCursorIterator = CreateDelegate<QueryCursorIteratorDelegate>(ProcQryCursorIterator);
            QryCursorClose = CreateDelegate<QueryCursorCloseDelegate>(ProcQryCursorClose);

            RELEASE = CreateDelegate<ReleaseDelegate>(ProcRelease);

            TxStart = CreateDelegate<TransactionsStartDelegate>(ProcTxStart);
            TxCommit = CreateDelegate<TransactionsCommitDelegate>(ProcTxCommit);
            TxCommitAsync = CreateDelegate<TransactionsCommitAsyncDelegate>(ProcTxCommitAsync);
            TxRollback = CreateDelegate<TransactionsRollbackDelegate>(ProcTxRollback);
            TxRollbackAsync = CreateDelegate<TransactionsRollbackAsyncDelegate>(ProcTxRollbackAsync);
            TxClose = CreateDelegate<TransactionsCloseDelegate>(ProcTxClose);
            TxState = CreateDelegate<TransactionsStateDelegate>(ProcTxState);
            TxSetRollbackOnly = CreateDelegate<TransactionsSetRollbackOnlyDelegate>(ProcTxSetRollbackOnly);
            TxResetMetrics = CreateDelegate<TransactionsResetMetricsDelegate>(ProcTxResetMetrics);

            THROW_TO_JAVA = CreateDelegate<ThrowToJavaDelegate>(ProcThrowToJava);

            HANDLERS_SIZE = CreateDelegate<HandlersSizeDelegate>(ProcHandlersSize);

            CREATE_CONTEXT = CreateDelegate<CreateContextDelegate>(ProcCreateContext);
            DELETE_CONTEXT = CreateDelegate<DeleteContextDelegate>(ProcDeleteContext);

            DESTROY_JVM = CreateDelegate<DestroyJvmDelegate>(ProcDestroyJvm);

            EVENTS_WITH_ASYNC = CreateDelegate<EventsWithAsyncDelegate>(ProcEventsWithAsync);
            EVENTS_STOP_LOCAL_LISTEN = CreateDelegate<EventsStopLocalListenDelegate>(ProcEventsStopLocalListen);
            EVENTS_LOCAL_LISTEN = CreateDelegate<EventsLocalListenDelegate>(ProcEventsLocalListen);
            EVENTS_IS_ENABLED = CreateDelegate<EventsIsEnabledDelegate>(ProcEventsIsEnabled);
            
            SERVICES_WITH_ASYNC = CreateDelegate<ServicesWithAsyncDelegate>(ProcServicesWithAsync);
            SERVICES_WITH_SERVER_KEEP_PORTABLE = CreateDelegate<ServicesWithServerKeepPortableDelegate>(ProcServicesWithServerKeepPortable);
            SERVICES_CANCEL = CreateDelegate<ServicesCancelDelegate>(ProcServicesCancel);
            SERVICES_CANCEL_ALL = CreateDelegate<ServicesCancelAllDelegate>(ProcServicesCancelAll);
            SERVICES_GET_SERVICE_PROXY = CreateDelegate<ServicesGetServiceProxyDelegate>(ProcServicesGetServiceProxy);
        }

        #region NATIVE METHODS: PROCESSOR

        internal static IUnmanagedTarget IgnitionStart(UnmanagedContext ctx, string cfgPath, string gridName,
            bool clientMode)
        {
            using (var mem = IgniteManager.Memory.Allocate().Stream())
            {
                mem.WriteBool(clientMode);

                sbyte* cfgPath0 = IgniteUtils.StringToUtf8Unmanaged(cfgPath);
                sbyte* gridName0 = IgniteUtils.StringToUtf8Unmanaged(gridName);

                try
                {
                    void* res = IGNITION_START(ctx.NativeContext, cfgPath0, gridName0, InteropFactoryId,
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
                return IGNITION_STOP(ctx, gridName0, cancel);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(gridName0));
            }
        }

        internal static void IgnitionStopAll(void* ctx, bool cancel)
        {
            IGNITION_STOP_ALL(ctx, cancel);
        }
        
        internal static void ProcessorReleaseStart(IUnmanagedTarget target)
        {
            PROCESSOR_RELEASE_START(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ProcessorProjection(IUnmanagedTarget target)
        {
            void* res = PROCESSOR_PROJECTION(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCache(IUnmanagedTarget target, string name)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = PROCESSOR_CACHE(target.Context, target.Target, name0);

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
                void* res = PROCESSOR_CREATE_CACHE(target.Context, target.Target, name0);

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
                void* res = PROCESSOR_GET_OR_CREATE_CACHE(target.Context, target.Target, name0);

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
                void* res = PROCESSOR_AFFINITY(target.Context, target.Target, name0);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }

        internal static IUnmanagedTarget ProcessorDataStreamer(IUnmanagedTarget target, string name, bool keepPortable)
        {
            sbyte* name0 = IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                void* res = PROCESSOR_DATA_STREAMER(target.Context, target.Target, name0, keepPortable);

                return target.ChangeTarget(res);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(name0));
            }
        }
        
        internal static IUnmanagedTarget ProcessorTransactions(IUnmanagedTarget target)
        {
            void* res = PROCESSOR_TRANSACTIONS(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorCompute(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = PROCESSOR_COMPUTE(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorMessage(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = PROCESSOR_MESSAGE(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorEvents(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = PROCESSOR_EVENTS(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorServices(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = PROCESSOR_SERVICES(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProcessorExtensions(IUnmanagedTarget target)
        {
            void* res = PROCESSOR_EXTENSIONS(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: TARGET

        internal static long TargetInStreamOutLong(IUnmanagedTarget target, int opType, long memPtr)
        {
            return TARGET_IN_STREAM_OUT_LONG(target.Context, target.Target, opType, memPtr);
        }

        internal static void TargetInStreamOutStream(IUnmanagedTarget target, int opType, long inMemPtr, long outMemPtr)
        {
            TARGET_IN_STREAM_OUT_STREAM(target.Context, target.Target, opType, inMemPtr, outMemPtr);
        }

        internal static IUnmanagedTarget TargetInStreamOutObject(IUnmanagedTarget target, int opType, long inMemPtr)
        {
            void* res = TARGET_IN_STREAM_OUT_OBJECT(target.Context, target.Target, opType, inMemPtr);

            return target.ChangeTarget(res);
        }

        internal static void TargetInObjectStreamOutStream(IUnmanagedTarget target, int opType, void* arg, long inMemPtr, long outMemPtr)
        {
            TARGET_IN_OBJECT_STREAM_OUT_STREAM(target.Context, target.Target, opType, arg, inMemPtr, outMemPtr);
        }

        internal static long TargetOutLong(IUnmanagedTarget target, int opType)
        {
            return TARGET_OUT_LONG(target.Context, target.Target, opType);
        }

        internal static void TargetOutStream(IUnmanagedTarget target, int opType, long memPtr)
        {
            TARGET_OUT_STREAM(target.Context, target.Target, opType, memPtr);
        }

        internal static IUnmanagedTarget TargetOutObject(IUnmanagedTarget target, int opType)
        {
            void* res = TARGET_OUT_OBJECT(target.Context, target.Target, opType);

            return target.ChangeTarget(res);
        }

        internal static void TargetListenFuture(IUnmanagedTarget target, long futId, int typ)
        {
            TargetListenFut(target.Context, target.Target, futId, typ);
        }

        internal static void TargetListenFutureForOperation(IUnmanagedTarget target, long futId, int typ, int opId)
        {
            TargetListenFutForOp(target.Context, target.Target, futId, typ, opId);
        }

        #endregion

        #region NATIVE METHODS: AFFINITY

        internal static int AffinityPartitions(IUnmanagedTarget target)
        {
            return AffinityParts(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: CACHE

        internal static IUnmanagedTarget CacheWithSkipStore(IUnmanagedTarget target)
        {
            void* res = CACHE_WITH_SKIP_STORE(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithNoRetries(IUnmanagedTarget target)
        {
            void* res = CACHE_WITH_NO_RETRIES(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithExpiryPolicy(IUnmanagedTarget target, long create, long update, long access)
        {
            void* res = CACHE_WITH_EXPIRY_POLICY(target.Context, target.Target, create, update, access);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithAsync(IUnmanagedTarget target)
        {
            void* res = CACHE_WITH_ASYNC(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheWithKeepPortable(IUnmanagedTarget target)
        {
            void* res = CACHE_WITH_KEEP_PORTABLE(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static void CacheClear(IUnmanagedTarget target)
        {
            CACHE_CLEAR(target.Context, target.Target);
        }

        internal static void CacheRemoveAll(IUnmanagedTarget target)
        {
            CACHE_REMOVE_ALL(target.Context, target.Target);
        }

        internal static IUnmanagedTarget CacheOutOpQueryCursor(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = CACHE_OUT_OP_QUERY_CURSOR(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheOutOpContinuousQuery(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = CACHE_OUT_OP_CONTINUOUS_QUERY(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheIterator(IUnmanagedTarget target)
        {
            void* res = CACHE_ITERATOR(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget CacheLocalIterator(IUnmanagedTarget target, int peekModes)
        {
            void* res = CACHE_LOCAL_ITERATOR(target.Context, target.Target, peekModes);

            return target.ChangeTarget(res);
        }

        internal static void CacheEnterLock(IUnmanagedTarget target, long id)
        {
            CACHE_ENTER_LOCK(target.Context, target.Target, id);
        }

        internal static void CacheExitLock(IUnmanagedTarget target, long id)
        {
            CACHE_EXIT_LOCK(target.Context, target.Target, id);
        }

        internal static bool CacheTryEnterLock(IUnmanagedTarget target, long id, long timeout)
        {
            return CACHE_TRY_ENTER_LOCK(target.Context, target.Target, id, timeout);
        }

        internal static void CacheCloseLock(IUnmanagedTarget target, long id)
        {
            CACHE_CLOSE_LOCK(target.Context, target.Target, id);
        }

        internal static void CacheRebalance(IUnmanagedTarget target, long futId)
        {
            CACHE_REBALANCE(target.Context, target.Target, futId);
        }

        internal static void CacheStoreCallbackInvoke(IUnmanagedTarget target, long memPtr)
        {
            CACHE_STORE_CALLBACK_INVOKE(target.Context, target.Target, memPtr);
        }

        internal static int CacheSize(IUnmanagedTarget target, int modes, bool loc)
        {
            return CACHE_SIZE(target.Context, target.Target, modes, loc);
        }

        #endregion

        #region NATIVE METHODS: COMPUTE

        internal static void ComputeWithNoFailover(IUnmanagedTarget target)
        {
            COMPUTE_WITH_NO_FAILOVER(target.Context, target.Target);
        }

        internal static void ComputeWithTimeout(IUnmanagedTarget target, long timeout)
        {
            COMPUTE_WITH_TIMEOUT(target.Context, target.Target, timeout);
        }

        internal static void ComputeExecuteNative(IUnmanagedTarget target, long taskPtr, long topVer)
        {
            COMPUTE_EXECUTE_NATIVE(target.Context, target.Target, taskPtr, topVer);
        }

        #endregion

        #region NATIVE METHODS: CONTINUOUS QUERY

        internal static void ContinuousQueryClose(IUnmanagedTarget target)
        {
            ContinuousQryClose(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ContinuousQueryGetInitialQueryCursor(IUnmanagedTarget target)
        {
            void* res = ContinuousQryGetInitialQueryCursor(target.Context, target.Target);

            return res == null ? null : target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: DATA STREAMER

        internal static void DataStreamerListenTopology(IUnmanagedTarget target, long ptr)
        {
            DataStreamerListenTop(target.Context, target.Target, ptr);
        }

        internal static bool DataStreamerAllowOverwriteGet(IUnmanagedTarget target)
        {
            return DATA_STREAMER_ALLOW_OVERWRITE_GET(target.Context, target.Target);
        }

        internal static void DataStreamerAllowOverwriteSet(IUnmanagedTarget target, bool val)
        {
            DATA_STREAMER_ALLOW_OVERWRITE_SET(target.Context, target.Target, val);
        }

        internal static bool DataStreamerSkipStoreGet(IUnmanagedTarget target)
        {
            return DATA_STREAMER_SKIP_STORE_GET(target.Context, target.Target);
        }

        internal static void DataStreamerSkipStoreSet(IUnmanagedTarget target, bool val)
        {
            DATA_STREAMER_SKIP_STORE_SET(target.Context, target.Target, val);
        }

        internal static int DataStreamerPerNodeBufferSizeGet(IUnmanagedTarget target)
        {
            return DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeBufferSizeSet(IUnmanagedTarget target, int val)
        {
            DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET(target.Context, target.Target, val);
        }

        internal static int DataStreamerPerNodeParallelOperationsGet(IUnmanagedTarget target)
        {
            return DataStreamerPerNodeParallelOpsGet(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeParallelOperationsSet(IUnmanagedTarget target, int val)
        {
            DataStreamerPerNodeParallelOpsSet(target.Context, target.Target, val);
        }

        #endregion

        #region NATIVE METHODS: MESSAGING

        internal static IUnmanagedTarget MessagingWithASync(IUnmanagedTarget target)
        {
            void* res = MessagingWithAsync(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: PROJECTION

        internal static IUnmanagedTarget ProjectionForOthers(IUnmanagedTarget target, IUnmanagedTarget prj)
        {
            void* res = PROJECTION_FOR_OTHERS(target.Context, target.Target, prj.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForRemotes(IUnmanagedTarget target)
        {
            void* res = PROJECTION_FOR_REMOTES(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForDaemons(IUnmanagedTarget target)
        {
            void* res = PROJECTION_FOR_DAEMONS(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForRandom(IUnmanagedTarget target)
        {
            void* res = PROJECTION_FOR_RANDOM(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForOldest(IUnmanagedTarget target)
        {
            void* res = PROJECTION_FOR_OLDEST(target.Context, target.Target);

            return target.ChangeTarget(res);
        }

        internal static IUnmanagedTarget ProjectionForYoungest(IUnmanagedTarget target)
        {
            void* res = PROJECTION_FOR_YOUNGEST(target.Context, target.Target);

            return target.ChangeTarget(res);
        }
        
        internal static void ProjectionResetMetrics(IUnmanagedTarget target)
        {
            PROJECTION_RESET_METRICS(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ProjectionOutOpRet(IUnmanagedTarget target, int type, long memPtr)
        {
            void* res = PROJECTION_OUT_OP_RET(target.Context, target.Target, type, memPtr);

            return target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: QUERY CURSOR

        internal static void QueryCursorIterator(IUnmanagedTarget target)
        {
            QryCursorIterator(target.Context, target.Target);
        }

        internal static void QueryCursorClose(IUnmanagedTarget target)
        {
            QryCursorClose(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: TRANSACTIONS

        internal static long TransactionsStart(IUnmanagedTarget target, int concurrency, int isolation, long timeout, int txSize)
        {
            return TxStart(target.Context, target.Target, concurrency, isolation, timeout, txSize);
        }

        internal static int TransactionsCommit(IUnmanagedTarget target, long id)
        {
            return TxCommit(target.Context, target.Target, id);
        }

        internal static void TransactionsCommitAsync(IUnmanagedTarget target, long id, long futId)
        {
            TxCommitAsync(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsRollback(IUnmanagedTarget target, long id)
        {
            return TxRollback(target.Context, target.Target, id);
        }

        internal static void TransactionsRollbackAsync(IUnmanagedTarget target, long id, long futId)
        {
            TxRollbackAsync(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsClose(IUnmanagedTarget target, long id)
        {
            return TxClose(target.Context, target.Target, id);
        }

        internal static int TransactionsState(IUnmanagedTarget target, long id)
        {
            return TxState(target.Context, target.Target, id);
        }

        internal static bool TransactionsSetRollbackOnly(IUnmanagedTarget target, long id)
        {
            return TxSetRollbackOnly(target.Context, target.Target, id);
        }

        internal static void TransactionsResetMetrics(IUnmanagedTarget target)
        {
            TxResetMetrics(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: MISCELANNEOUS

        internal static void Reallocate(long memPtr, int cap)
        {
            int res = REALLOCATE(memPtr, cap);

            if (res != 0)
                throw new IgniteException("Failed to reallocate external memory [ptr=" + memPtr + 
                    ", capacity=" + cap + ']');
        }

        internal static void Release(IUnmanagedTarget target)
        {
            RELEASE(target.Target);
        }

        internal static void ThrowToJava(void* ctx, Exception e)
        {
            char* msgChars = (char*)IgniteUtils.StringToUtf8Unmanaged(e.Message);

            try
            {
                THROW_TO_JAVA(ctx, msgChars);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(msgChars));
            }
        }

        

        internal static int HandlersSize()
        {
            return HANDLERS_SIZE();
        }

        internal static void* CreateContext(void* opts, int optsLen, void* cbs)
        {
            return CREATE_CONTEXT(opts, optsLen, cbs);
        }

        internal static void DeleteContext(void* ctx)
        {
            DELETE_CONTEXT(ctx);
        }

        internal static void DestroyJvm(void* ctx)
        {
            DESTROY_JVM(ctx);
        }

        #endregion

        #region NATIVE METHODS: EVENTS

        internal static IUnmanagedTarget EventsWithAsync(IUnmanagedTarget target)
        {
            return target.ChangeTarget(EVENTS_WITH_ASYNC(target.Context, target.Target));
        }

        internal static bool EventsStopLocalListen(IUnmanagedTarget target, long handle)
        {
            return EVENTS_STOP_LOCAL_LISTEN(target.Context, target.Target, handle);
        }

        internal static bool EventsIsEnabled(IUnmanagedTarget target, int type)
        {
            return EVENTS_IS_ENABLED(target.Context, target.Target, type);
        }

        internal static void EventsLocalListen(IUnmanagedTarget target, long handle, int type)
        {
            EVENTS_LOCAL_LISTEN(target.Context, target.Target, handle, type);
        }

        #endregion

        #region NATIVE METHODS: SERVICES

        internal static IUnmanagedTarget ServicesWithAsync(IUnmanagedTarget target)
        {
            return target.ChangeTarget(SERVICES_WITH_ASYNC(target.Context, target.Target));
        }

        internal static IUnmanagedTarget ServicesWithServerKeepPortable(IUnmanagedTarget target)
        {
            return target.ChangeTarget(SERVICES_WITH_SERVER_KEEP_PORTABLE(target.Context, target.Target));
        }

        internal static void ServicesCancel(IUnmanagedTarget target, string name)
        {
            var nameChars = (char*)IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                SERVICES_CANCEL(target.Context, target.Target, nameChars);
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(nameChars));
            }
        }

        internal static void ServicesCancelAll(IUnmanagedTarget target)
        {
            SERVICES_CANCEL_ALL(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ServicesGetServiceProxy(IUnmanagedTarget target, string name, bool sticky)
        {
            var nameChars = (char*)IgniteUtils.StringToUtf8Unmanaged(name);

            try
            {
                return target.ChangeTarget(
                    SERVICES_GET_SERVICE_PROXY(target.Context, target.Target, nameChars, sticky));
            }
            finally
            {
                Marshal.FreeHGlobal(new IntPtr(nameChars));
            }
        }

        #endregion

        /// <summary>
        /// No-op initializer used to force type loading and static constructor call.
        /// </summary>
        internal static void Initialize()
        {
            // No-op.
        }

        /// <summary>
        /// Create delegate for the given procedure.
        /// </summary>
        /// <typeparam name="T">Delegate type.</typeparam>
        /// <param name="procName">Procedure name.</param>
        /// <returns></returns>
        private static T CreateDelegate<T>(string procName)
        {
            var procPtr = NativeMethods.GetProcAddress(Ptr, procName);

            if (procPtr == IntPtr.Zero)
                throw new IgniteException(string.Format("Unable to find native function: {0} (Error code: {1}). " +
                                                      "Make sure that module.def is up to date",
                    procName, Marshal.GetLastWin32Error()));

            return TypeCaster<T>.Cast(Marshal.GetDelegateForFunctionPointer(procPtr, typeof (T)));
        }
    }
}
