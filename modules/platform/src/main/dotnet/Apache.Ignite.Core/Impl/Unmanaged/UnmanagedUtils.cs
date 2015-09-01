/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using U = Apache.Ignite.Core.Impl.GridUtils;

    /// <summary>
    /// Unmanaged utility classes.
    /// </summary>
    internal static unsafe class UnmanagedUtils
    {
        /** Interop factory ID for .Net. */
        private const int INTEROP_FACTORY_ID = 1001;

        #region PROCEDURE NAMES

        private const string PROC_REALLOCATE = "IgniteReallocate";

        private const string PROC_IGNITION_START = "IgniteIgnitionStart";
        private const string PROC_IGNITION_STOP = "IgniteIgnitionStop";
        private const string PROC_IGNITION_STOP_ALL = "IgniteIgnitionStopAll";
        
        private const string PROC_PROCESSOR_RELEASE_START = "IgniteProcessorReleaseStart";
        private const string PROC_PROCESSOR_PROJECTION = "IgniteProcessorProjection";
        private const string PROC_PROCESSOR_CACHE = "IgniteProcessorCache";
        private const string PROC_PROCESSOR_GET_OR_CREATE_CACHE = "IgniteProcessorGetOrCreateCache";
        private const string PROC_PROCESSOR_CREATE_CACHE = "IgniteProcessorCreateCache";
        private const string PROC_PROCESSOR_AFFINITY = "IgniteProcessorAffinity";
        private const string PROC_PROCESSOR_DATA_STREAMER = "IgniteProcessorDataStreamer";
        private const string PROC_PROCESSOR_TRANSACTIONS = "IgniteProcessorTransactions";
        private const string PROC_PROCESSOR_COMPUTE = "IgniteProcessorCompute";
        private const string PROC_PROCESSOR_MESSAGE = "IgniteProcessorMessage";
        private const string PROC_PROCESSOR_EVENTS = "IgniteProcessorEvents";
        private const string PROC_PROCESSOR_SERVICES = "IgniteProcessorServices";
        private const string PROC_PROCESSOR_EXTENSIONS = "IgniteProcessorExtensions";
        
        private const string PROC_TARGET_IN_STREAM_OUT_LONG = "IgniteTargetInStreamOutLong";
        private const string PROC_TARGET_IN_STREAM_OUT_STREAM = "IgniteTargetInStreamOutStream";
        private const string PROC_TARGET_IN_STREAM_OUT_OBJECT = "IgniteTargetInStreamOutObject";
        private const string PROC_TARGET_IN_OBJECT_STREAM_OUT_STREAM = "IgniteTargetInObjectStreamOutStream";
        private const string PROC_TARGET_OUT_LONG = "IgniteTargetOutLong";
        private const string PROC_TARGET_OUT_STREAM = "IgniteTargetOutStream";
        private const string PROC_TARGET_OUT_OBJECT = "IgniteTargetOutObject";
        private const string PROC_TARGET_LISTEN_FUT = "IgniteTargetListenFuture";
        private const string PROC_TARGET_LISTEN_FUT_FOR_OP = "IgniteTargetListenFutureForOperation";

        private const string PROC_AFFINITY_PARTS = "IgniteAffinityPartitions";

        private const string PROC_CACHE_WITH_SKIP_STORE = "IgniteCacheWithSkipStore";
        private const string PROC_CACHE_WITH_NO_RETRIES = "IgniteCacheWithNoRetries";
        private const string PROC_CACHE_WITH_EXPIRY_POLICY = "IgniteCacheWithExpiryPolicy";
        private const string PROC_CACHE_WITH_ASYNC = "IgniteCacheWithAsync";
        private const string PROC_CACHE_WITH_KEEP_PORTABLE = "IgniteCacheWithKeepPortable";
        private const string PROC_CACHE_CLEAR = "IgniteCacheClear";
        private const string PROC_CACHE_REMOVE_ALL = "IgniteCacheRemoveAll";
        private const string PROC_CACHE_OUT_OP_QUERY_CURSOR = "IgniteCacheOutOpQueryCursor";
        private const string PROC_CACHE_OUT_OP_CONTINUOUS_QUERY = "IgniteCacheOutOpContinuousQuery";
        private const string PROC_CACHE_ITERATOR = "IgniteCacheIterator";
        private const string PROC_CACHE_LOCAL_ITERATOR = "IgniteCacheLocalIterator";
        private const string PROC_CACHE_ENTER_LOCK = "IgniteCacheEnterLock";
        private const string PROC_CACHE_EXIT_LOCK = "IgniteCacheExitLock";
        private const string PROC_CACHE_TRY_ENTER_LOCK = "IgniteCacheTryEnterLock";
        private const string PROC_CACHE_CLOSE_LOCK = "IgniteCacheCloseLock";
        private const string PROC_CACHE_REBALANCE = "IgniteCacheRebalance";
        private const string PROC_CACHE_SIZE = "IgniteCacheSize";

        private const string PROC_CACHE_STORE_CALLBACK_INVOKE = "IgniteCacheStoreCallbackInvoke";

        private const string PROC_COMPUTE_WITH_NO_FAILOVER = "IgniteComputeWithNoFailover";
        private const string PROC_COMPUTE_WITH_TIMEOUT = "IgniteComputeWithTimeout";
        private const string PROC_COMPUTE_EXECUTE_NATIVE = "IgniteComputeExecuteNative";

        private const string PROC_CONTINUOUS_QRY_CLOSE = "IgniteContinuousQueryClose";
        private const string PROC_CONTINUOUS_QRY_GET_INITIAL_QUERY_CURSOR = "IgniteContinuousQueryGetInitialQueryCursor";

        private const string PROC_DATA_STREAMER_LISTEN_TOP = "IgniteDataStreamerListenTopology";
        private const string PROC_DATA_STREAMER_ALLOW_OVERWRITE_GET = "IgniteDataStreamerAllowOverwriteGet";
        private const string PROC_DATA_STREAMER_ALLOW_OVERWRITE_SET = "IgniteDataStreamerAllowOverwriteSet";
        private const string PROC_DATA_STREAMER_SKIP_STORE_GET = "IgniteDataStreamerSkipStoreGet";
        private const string PROC_DATA_STREAMER_SKIP_STORE_SET = "IgniteDataStreamerSkipStoreSet";
        private const string PROC_DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET = "IgniteDataStreamerPerNodeBufferSizeGet";
        private const string PROC_DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET = "IgniteDataStreamerPerNodeBufferSizeSet";
        private const string PROC_DATA_STREAMER_PER_NODE_PARALLEL_OPS_GET = "IgniteDataStreamerPerNodeParallelOperationsGet";
        private const string PROC_DATA_STREAMER_PER_NODE_PARALLEL_OPS_SET = "IgniteDataStreamerPerNodeParallelOperationsSet";

        private const string PROC_MESSAGING_WITH_ASYNC = "IgniteMessagingWithAsync";

        private const string PROC_QRY_CURSOR_ITERATOR = "IgniteQueryCursorIterator";
        private const string PROC_QRY_CURSOR_CLOSE = "IgniteQueryCursorClose";

        private const string PROC_PROJECTION_FOR_OTHERS = "IgniteProjectionForOthers";
        private const string PROC_PROJECTION_FOR_REMOTES = "IgniteProjectionForRemotes";
        private const string PROC_PROJECTION_FOR_DAEMONS = "IgniteProjectionForDaemons";
        private const string PROC_PROJECTION_FOR_RANDOM = "IgniteProjectionForRandom";
        private const string PROC_PROJECTION_FOR_OLDEST = "IgniteProjectionForOldest";
        private const string PROC_PROJECTION_FOR_YOUNGEST = "IgniteProjectionForYoungest";
        private const string PROC_PROJECTION_RESET_METRICS = "IgniteProjectionResetMetrics";
        private const string PROC_PROJECTION_OUT_OP_RET = "IgniteProjectionOutOpRet";

        private const string PROC_RELEASE = "IgniteRelease";

        private const string PROC_TX_START = "IgniteTransactionsStart";
        private const string PROC_TX_COMMIT = "IgniteTransactionsCommit";
        private const string PROC_TX_COMMIT_ASYNC = "IgniteTransactionsCommitAsync";
        private const string PROC_TX_ROLLBACK = "IgniteTransactionsRollback";
        private const string PROC_TX_ROLLBACK_ASYNC = "IgniteTransactionsRollbackAsync";
        private const string PROC_TX_CLOSE = "IgniteTransactionsClose";
        private const string PROC_TX_STATE = "IgniteTransactionsState";
        private const string PROC_TX_SET_ROLLBACK_ONLY = "IgniteTransactionsSetRollbackOnly";
        private const string PROC_TX_RESET_METRICS = "IgniteTransactionsResetMetrics";

        private const string PROC_THROW_TO_JAVA = "IgniteThrowToJava";

        private const string PROC_DESTROY_JVM = "IgniteDestroyJvm";

        private const string PROC_HANDLERS_SIZE = "IgniteHandlersSize";

        private const string PROC_CREATE_CONTEXT = "IgniteCreateContext";
        
        private const string PROC_EVENTS_WITH_ASYNC = "IgniteEventsWithAsync";
        private const string PROC_EVENTS_STOP_LOCAL_LISTEN = "IgniteEventsStopLocalListen";
        private const string PROC_EVENTS_LOCAL_LISTEN = "IgniteEventsLocalListen";
        private const string PROC_EVENTS_IS_ENABLED = "IgniteEventsIsEnabled";

        private const string PROC_DELETE_CONTEXT = "IgniteDeleteContext";
        
        private const string PROC_SERVICES_WITH_ASYNC = "IgniteServicesWithAsync";
        private const string PROC_SERVICES_WITH_SERVER_KEEP_PORTABLE = "IgniteServicesWithServerKeepPortable";
        private const string PROC_SERVICES_CANCEL = "IgniteServicesCancel";
        private const string PROC_SERVICES_CANCEL_ALL = "IgniteServicesCancelAll";
        private const string PROC_SERVICES_GET_SERVICE_PROXY = "IgniteServicesGetServiceProxy";
        
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
        private static readonly TargetListenFutureDelegate TARGET_LISTEN_FUT;
        private static readonly TargetListenFutureForOpDelegate TARGET_LISTEN_FUT_FOR_OP;

        private static readonly AffinityPartitionsDelegate AFFINITY_PARTS;

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

        private static readonly ContinuousQueryCloseDelegate CONTINUOUS_QRY_CLOSE;
        private static readonly ContinuousQueryGetInitialQueryCursorDelegate CONTINUOUS_QRY_GET_INITIAL_QUERY_CURSOR;

        private static readonly DataStreamerListenTopologyDelegate DATA_STREAMER_LISTEN_TOP;
        private static readonly DataStreamerAllowOverwriteGetDelegate DATA_STREAMER_ALLOW_OVERWRITE_GET;
        private static readonly DataStreamerAllowOverwriteSetDelegate DATA_STREAMER_ALLOW_OVERWRITE_SET;
        private static readonly DataStreamerSkipStoreGetDelegate DATA_STREAMER_SKIP_STORE_GET;
        private static readonly DataStreamerSkipStoreSetDelegate DATA_STREAMER_SKIP_STORE_SET;
        private static readonly DataStreamerPerNodeBufferSizeGetDelegate DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET;
        private static readonly DataStreamerPerNodeBufferSizeSetDelegate DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET;
        private static readonly DataStreamerPerNodeParallelOperationsGetDelegate DATA_STREAMER_PER_NODE_PARALLEL_OPS_GET;
        private static readonly DataStreamerPerNodeParallelOperationsSetDelegate DATA_STREAMER_PER_NODE_PARALLEL_OPS_SET;

        private static readonly MessagingWithAsyncDelegate MESSAGING_WITH_ASYNC;

        private static readonly ProjectionForOthersDelegate PROJECTION_FOR_OTHERS;
        private static readonly ProjectionForRemotesDelegate PROJECTION_FOR_REMOTES;
        private static readonly ProjectionForDaemonsDelegate PROJECTION_FOR_DAEMONS;
        private static readonly ProjectionForRandomDelegate PROJECTION_FOR_RANDOM;
        private static readonly ProjectionForOldestDelegate PROJECTION_FOR_OLDEST;
        private static readonly ProjectionForYoungestDelegate PROJECTION_FOR_YOUNGEST;
        private static readonly ProjectionResetMetricsDelegate PROJECTION_RESET_METRICS;
        private static readonly ProjectionOutOpRetDelegate PROJECTION_OUT_OP_RET;

        private static readonly QueryCursorIteratorDelegate QRY_CURSOR_ITERATOR;
        private static readonly QueryCursorCloseDelegate QRY_CURSOR_CLOSE;

        private static readonly ReleaseDelegate RELEASE;

        private static readonly TransactionsStartDelegate TX_START;
        private static readonly TransactionsCommitDelegate TX_COMMIT;
        private static readonly TransactionsCommitAsyncDelegate TX_COMMIT_ASYNC;
        private static readonly TransactionsRollbackDelegate TX_ROLLBACK;
        private static readonly TransactionsRollbackAsyncDelegate TX_ROLLBACK_ASYNC;
        private static readonly TransactionsCloseDelegate TX_CLOSE;
        private static readonly TransactionsStateDelegate TX_STATE;
        private static readonly TransactionsSetRollbackOnlyDelegate TX_SET_ROLLBACK_ONLY;
        private static readonly TransactionsResetMetricsDelegate TX_RESET_METRICS;

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

        #endregion

        /** Library pointer. */
        private static readonly IntPtr PTR;

        /// <summary>
        /// Initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        static UnmanagedUtils()
        {
            var path = U.UnpackEmbeddedResource(U.FILE_GG_JNI_DLL);

            PTR = NativeMethods.LoadLibrary(path);

            if (PTR == IntPtr.Zero)
                throw new IgniteException("Failed to load " + U.FILE_GG_JNI_DLL + ": " + Marshal.GetLastWin32Error());

            REALLOCATE = CreateDelegate<ReallocateDelegate>(PROC_REALLOCATE);

            IGNITION_START = CreateDelegate<IgnitionStartDelegate>(PROC_IGNITION_START);
            IGNITION_STOP = CreateDelegate<IgnitionStopDelegate>(PROC_IGNITION_STOP);
            IGNITION_STOP_ALL = CreateDelegate<IgnitionStopAllDelegate>(PROC_IGNITION_STOP_ALL);
            
            PROCESSOR_RELEASE_START = CreateDelegate<ProcessorReleaseStartDelegate>(PROC_PROCESSOR_RELEASE_START);
            PROCESSOR_PROJECTION = CreateDelegate<ProcessorProjectionDelegate>(PROC_PROCESSOR_PROJECTION);
            PROCESSOR_CACHE = CreateDelegate<ProcessorCacheDelegate>(PROC_PROCESSOR_CACHE);
            PROCESSOR_CREATE_CACHE = CreateDelegate<ProcessorCreateCacheDelegate>(PROC_PROCESSOR_CREATE_CACHE);
            PROCESSOR_GET_OR_CREATE_CACHE = CreateDelegate<ProcessorGetOrCreateCacheDelegate>(PROC_PROCESSOR_GET_OR_CREATE_CACHE);
            PROCESSOR_AFFINITY = CreateDelegate<ProcessorAffinityDelegate>(PROC_PROCESSOR_AFFINITY);
            PROCESSOR_DATA_STREAMER = CreateDelegate<ProcessorDataStreamerDelegate>(PROC_PROCESSOR_DATA_STREAMER);
            PROCESSOR_TRANSACTIONS = CreateDelegate<ProcessorTransactionsDelegate>(PROC_PROCESSOR_TRANSACTIONS);
            PROCESSOR_COMPUTE = CreateDelegate<ProcessorComputeDelegate>(PROC_PROCESSOR_COMPUTE);
            PROCESSOR_MESSAGE = CreateDelegate<ProcessorMessageDelegate>(PROC_PROCESSOR_MESSAGE);
            PROCESSOR_EVENTS = CreateDelegate<ProcessorEventsDelegate>(PROC_PROCESSOR_EVENTS);
            PROCESSOR_SERVICES = CreateDelegate<ProcessorServicesDelegate>(PROC_PROCESSOR_SERVICES);
            PROCESSOR_EXTENSIONS = CreateDelegate<ProcessorExtensionsDelegate>(PROC_PROCESSOR_EXTENSIONS);
            
            TARGET_IN_STREAM_OUT_LONG = CreateDelegate<TargetInStreamOutLongDelegate>(PROC_TARGET_IN_STREAM_OUT_LONG);
            TARGET_IN_STREAM_OUT_STREAM = CreateDelegate<TargetInStreamOutStreamDelegate>(PROC_TARGET_IN_STREAM_OUT_STREAM);
            TARGET_IN_STREAM_OUT_OBJECT = CreateDelegate<TargetInStreamOutObjectDelegate>(PROC_TARGET_IN_STREAM_OUT_OBJECT);
            TARGET_IN_OBJECT_STREAM_OUT_STREAM = CreateDelegate<TargetInObjectStreamOutStreamDelegate>(PROC_TARGET_IN_OBJECT_STREAM_OUT_STREAM);
            TARGET_OUT_LONG = CreateDelegate<TargetOutLongDelegate>(PROC_TARGET_OUT_LONG);
            TARGET_OUT_STREAM = CreateDelegate<TargetOutStreamDelegate>(PROC_TARGET_OUT_STREAM);
            TARGET_OUT_OBJECT = CreateDelegate<TargetOutObjectDelegate>(PROC_TARGET_OUT_OBJECT);
            TARGET_LISTEN_FUT = CreateDelegate<TargetListenFutureDelegate>(PROC_TARGET_LISTEN_FUT);
            TARGET_LISTEN_FUT_FOR_OP = CreateDelegate<TargetListenFutureForOpDelegate>(PROC_TARGET_LISTEN_FUT_FOR_OP);

            AFFINITY_PARTS = CreateDelegate<AffinityPartitionsDelegate>(PROC_AFFINITY_PARTS);

            CACHE_WITH_SKIP_STORE = CreateDelegate<CacheWithSkipStoreDelegate>(PROC_CACHE_WITH_SKIP_STORE);
            CACHE_WITH_NO_RETRIES = CreateDelegate<CacheNoRetriesDelegate>(PROC_CACHE_WITH_NO_RETRIES);
            CACHE_WITH_EXPIRY_POLICY = CreateDelegate<CacheWithExpiryPolicyDelegate>(PROC_CACHE_WITH_EXPIRY_POLICY);
            CACHE_WITH_ASYNC = CreateDelegate<CacheWithAsyncDelegate>(PROC_CACHE_WITH_ASYNC);
            CACHE_WITH_KEEP_PORTABLE = CreateDelegate<CacheWithKeepPortableDelegate>(PROC_CACHE_WITH_KEEP_PORTABLE);
            CACHE_CLEAR = CreateDelegate<CacheClearDelegate>(PROC_CACHE_CLEAR);
            CACHE_REMOVE_ALL = CreateDelegate<CacheRemoveAllDelegate>(PROC_CACHE_REMOVE_ALL);
            CACHE_OUT_OP_QUERY_CURSOR = CreateDelegate<CacheOutOpQueryCursorDelegate>(PROC_CACHE_OUT_OP_QUERY_CURSOR);
            CACHE_OUT_OP_CONTINUOUS_QUERY = CreateDelegate<CacheOutOpContinuousQueryDelegate>(PROC_CACHE_OUT_OP_CONTINUOUS_QUERY);
            CACHE_ITERATOR = CreateDelegate<CacheIteratorDelegate>(PROC_CACHE_ITERATOR);
            CACHE_LOCAL_ITERATOR = CreateDelegate<CacheLocalIteratorDelegate>(PROC_CACHE_LOCAL_ITERATOR);
            CACHE_ENTER_LOCK = CreateDelegate<CacheEnterLockDelegate>(PROC_CACHE_ENTER_LOCK);
            CACHE_EXIT_LOCK = CreateDelegate<CacheExitLockDelegate>(PROC_CACHE_EXIT_LOCK);
            CACHE_TRY_ENTER_LOCK = CreateDelegate<CacheTryEnterLockDelegate>(PROC_CACHE_TRY_ENTER_LOCK);
            CACHE_CLOSE_LOCK = CreateDelegate<CacheCloseLockDelegate>(PROC_CACHE_CLOSE_LOCK);
            CACHE_REBALANCE = CreateDelegate<CacheRebalanceDelegate>(PROC_CACHE_REBALANCE);
            CACHE_SIZE = CreateDelegate<CacheSizeDelegate>(PROC_CACHE_SIZE);

            CACHE_STORE_CALLBACK_INVOKE = CreateDelegate<CacheStoreCallbackInvokeDelegate>(PROC_CACHE_STORE_CALLBACK_INVOKE);

            COMPUTE_WITH_NO_FAILOVER = CreateDelegate<ComputeWithNoFailoverDelegate>(PROC_COMPUTE_WITH_NO_FAILOVER);
            COMPUTE_WITH_TIMEOUT = CreateDelegate<ComputeWithTimeoutDelegate>(PROC_COMPUTE_WITH_TIMEOUT);
            COMPUTE_EXECUTE_NATIVE = CreateDelegate<ComputeExecuteNativeDelegate>(PROC_COMPUTE_EXECUTE_NATIVE);

            CONTINUOUS_QRY_CLOSE = CreateDelegate<ContinuousQueryCloseDelegate>(PROC_CONTINUOUS_QRY_CLOSE);
            CONTINUOUS_QRY_GET_INITIAL_QUERY_CURSOR = CreateDelegate<ContinuousQueryGetInitialQueryCursorDelegate>(PROC_CONTINUOUS_QRY_GET_INITIAL_QUERY_CURSOR);

            DATA_STREAMER_LISTEN_TOP = CreateDelegate<DataStreamerListenTopologyDelegate>(PROC_DATA_STREAMER_LISTEN_TOP); 
            DATA_STREAMER_ALLOW_OVERWRITE_GET = CreateDelegate<DataStreamerAllowOverwriteGetDelegate>(PROC_DATA_STREAMER_ALLOW_OVERWRITE_GET);
            DATA_STREAMER_ALLOW_OVERWRITE_SET = CreateDelegate<DataStreamerAllowOverwriteSetDelegate>(PROC_DATA_STREAMER_ALLOW_OVERWRITE_SET); 
            DATA_STREAMER_SKIP_STORE_GET = CreateDelegate<DataStreamerSkipStoreGetDelegate>(PROC_DATA_STREAMER_SKIP_STORE_GET); 
            DATA_STREAMER_SKIP_STORE_SET = CreateDelegate<DataStreamerSkipStoreSetDelegate>(PROC_DATA_STREAMER_SKIP_STORE_SET); 
            DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET = CreateDelegate<DataStreamerPerNodeBufferSizeGetDelegate>(PROC_DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET); 
            DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET = CreateDelegate<DataStreamerPerNodeBufferSizeSetDelegate>(PROC_DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET); 
            DATA_STREAMER_PER_NODE_PARALLEL_OPS_GET = CreateDelegate<DataStreamerPerNodeParallelOperationsGetDelegate>(PROC_DATA_STREAMER_PER_NODE_PARALLEL_OPS_GET); 
            DATA_STREAMER_PER_NODE_PARALLEL_OPS_SET = CreateDelegate<DataStreamerPerNodeParallelOperationsSetDelegate>(PROC_DATA_STREAMER_PER_NODE_PARALLEL_OPS_SET); 

            MESSAGING_WITH_ASYNC = CreateDelegate<MessagingWithAsyncDelegate>(PROC_MESSAGING_WITH_ASYNC);

            PROJECTION_FOR_OTHERS = CreateDelegate<ProjectionForOthersDelegate>(PROC_PROJECTION_FOR_OTHERS);
            PROJECTION_FOR_REMOTES = CreateDelegate<ProjectionForRemotesDelegate>(PROC_PROJECTION_FOR_REMOTES);
            PROJECTION_FOR_DAEMONS = CreateDelegate<ProjectionForDaemonsDelegate>(PROC_PROJECTION_FOR_DAEMONS);
            PROJECTION_FOR_RANDOM = CreateDelegate<ProjectionForRandomDelegate>(PROC_PROJECTION_FOR_RANDOM);
            PROJECTION_FOR_OLDEST = CreateDelegate<ProjectionForOldestDelegate>(PROC_PROJECTION_FOR_OLDEST);
            PROJECTION_FOR_YOUNGEST = CreateDelegate<ProjectionForYoungestDelegate>(PROC_PROJECTION_FOR_YOUNGEST);
            PROJECTION_RESET_METRICS = CreateDelegate<ProjectionResetMetricsDelegate>(PROC_PROJECTION_RESET_METRICS);
            PROJECTION_OUT_OP_RET = CreateDelegate<ProjectionOutOpRetDelegate>(PROC_PROJECTION_OUT_OP_RET);

            QRY_CURSOR_ITERATOR = CreateDelegate<QueryCursorIteratorDelegate>(PROC_QRY_CURSOR_ITERATOR);
            QRY_CURSOR_CLOSE = CreateDelegate<QueryCursorCloseDelegate>(PROC_QRY_CURSOR_CLOSE);

            RELEASE = CreateDelegate<ReleaseDelegate>(PROC_RELEASE);

            TX_START = CreateDelegate<TransactionsStartDelegate>(PROC_TX_START);
            TX_COMMIT = CreateDelegate<TransactionsCommitDelegate>(PROC_TX_COMMIT);
            TX_COMMIT_ASYNC = CreateDelegate<TransactionsCommitAsyncDelegate>(PROC_TX_COMMIT_ASYNC);
            TX_ROLLBACK = CreateDelegate<TransactionsRollbackDelegate>(PROC_TX_ROLLBACK);
            TX_ROLLBACK_ASYNC = CreateDelegate<TransactionsRollbackAsyncDelegate>(PROC_TX_ROLLBACK_ASYNC);
            TX_CLOSE = CreateDelegate<TransactionsCloseDelegate>(PROC_TX_CLOSE);
            TX_STATE = CreateDelegate<TransactionsStateDelegate>(PROC_TX_STATE);
            TX_SET_ROLLBACK_ONLY = CreateDelegate<TransactionsSetRollbackOnlyDelegate>(PROC_TX_SET_ROLLBACK_ONLY);
            TX_RESET_METRICS = CreateDelegate<TransactionsResetMetricsDelegate>(PROC_TX_RESET_METRICS);

            THROW_TO_JAVA = CreateDelegate<ThrowToJavaDelegate>(PROC_THROW_TO_JAVA);

            HANDLERS_SIZE = CreateDelegate<HandlersSizeDelegate>(PROC_HANDLERS_SIZE);

            CREATE_CONTEXT = CreateDelegate<CreateContextDelegate>(PROC_CREATE_CONTEXT);
            DELETE_CONTEXT = CreateDelegate<DeleteContextDelegate>(PROC_DELETE_CONTEXT);

            DESTROY_JVM = CreateDelegate<DestroyJvmDelegate>(PROC_DESTROY_JVM);

            EVENTS_WITH_ASYNC = CreateDelegate<EventsWithAsyncDelegate>(PROC_EVENTS_WITH_ASYNC);
            EVENTS_STOP_LOCAL_LISTEN = CreateDelegate<EventsStopLocalListenDelegate>(PROC_EVENTS_STOP_LOCAL_LISTEN);
            EVENTS_LOCAL_LISTEN = CreateDelegate<EventsLocalListenDelegate>(PROC_EVENTS_LOCAL_LISTEN);
            EVENTS_IS_ENABLED = CreateDelegate<EventsIsEnabledDelegate>(PROC_EVENTS_IS_ENABLED);
            
            SERVICES_WITH_ASYNC = CreateDelegate<ServicesWithAsyncDelegate>(PROC_SERVICES_WITH_ASYNC);
            SERVICES_WITH_SERVER_KEEP_PORTABLE = CreateDelegate<ServicesWithServerKeepPortableDelegate>(PROC_SERVICES_WITH_SERVER_KEEP_PORTABLE);
            SERVICES_CANCEL = CreateDelegate<ServicesCancelDelegate>(PROC_SERVICES_CANCEL);
            SERVICES_CANCEL_ALL = CreateDelegate<ServicesCancelAllDelegate>(PROC_SERVICES_CANCEL_ALL);
            SERVICES_GET_SERVICE_PROXY = CreateDelegate<ServicesGetServiceProxyDelegate>(PROC_SERVICES_GET_SERVICE_PROXY);
        }

        #region NATIVE METHODS: PROCESSOR

        internal static IUnmanagedTarget IgnitionStart(UnmanagedContext ctx, string cfgPath, string gridName, 
            bool clientMode)
        {
            using (var mem = GridManager.Memory.Allocate().Stream())
            {
                mem.WriteBool(clientMode);

                sbyte* cfgPath0 = GridUtils.StringToUtf8Unmanaged(cfgPath);
                sbyte* gridName0 = GridUtils.StringToUtf8Unmanaged(gridName);

                try
                {
                    void* res = IGNITION_START(ctx.NativeContext, cfgPath0, gridName0, INTEROP_FACTORY_ID,
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
            sbyte* gridName0 = GridUtils.StringToUtf8Unmanaged(gridName);

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
            sbyte* name0 = GridUtils.StringToUtf8Unmanaged(name);

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
            sbyte* name0 = GridUtils.StringToUtf8Unmanaged(name);

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
            sbyte* name0 = GridUtils.StringToUtf8Unmanaged(name);

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
            sbyte* name0 = GridUtils.StringToUtf8Unmanaged(name);

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
            sbyte* name0 = GridUtils.StringToUtf8Unmanaged(name);

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
            TARGET_LISTEN_FUT(target.Context, target.Target, futId, typ);
        }

        internal static void TargetListenFutureForOperation(IUnmanagedTarget target, long futId, int typ, int opId)
        {
            TARGET_LISTEN_FUT_FOR_OP(target.Context, target.Target, futId, typ, opId);
        }

        #endregion

        #region NATIVE METHODS: AFFINITY

        internal static int AffinityPartitions(IUnmanagedTarget target)
        {
            return AFFINITY_PARTS(target.Context, target.Target);
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
            CONTINUOUS_QRY_CLOSE(target.Context, target.Target);
        }

        internal static IUnmanagedTarget ContinuousQueryGetInitialQueryCursor(IUnmanagedTarget target)
        {
            void* res = CONTINUOUS_QRY_GET_INITIAL_QUERY_CURSOR(target.Context, target.Target);

            return res == null ? null : target.ChangeTarget(res);
        }

        #endregion

        #region NATIVE METHODS: DATA STREAMER

        internal static void DataStreamerListenTopology(IUnmanagedTarget target, long ptr)
        {
            DATA_STREAMER_LISTEN_TOP(target.Context, target.Target, ptr);
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
            return DATA_STREAMER_PER_NODE_PARALLEL_OPS_GET(target.Context, target.Target);
        }

        internal static void DataStreamerPerNodeParallelOperationsSet(IUnmanagedTarget target, int val)
        {
            DATA_STREAMER_PER_NODE_PARALLEL_OPS_SET(target.Context, target.Target, val);
        }

        #endregion

        #region NATIVE METHODS: MESSAGING

        internal static IUnmanagedTarget MessagingWithASync(IUnmanagedTarget target)
        {
            void* res = MESSAGING_WITH_ASYNC(target.Context, target.Target);

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
            QRY_CURSOR_ITERATOR(target.Context, target.Target);
        }

        internal static void QueryCursorClose(IUnmanagedTarget target)
        {
            QRY_CURSOR_CLOSE(target.Context, target.Target);
        }

        #endregion

        #region NATIVE METHODS: TRANSACTIONS

        internal static long TransactionsStart(IUnmanagedTarget target, int concurrency, int isolation, long timeout, int txSize)
        {
            return TX_START(target.Context, target.Target, concurrency, isolation, timeout, txSize);
        }

        internal static int TransactionsCommit(IUnmanagedTarget target, long id)
        {
            return TX_COMMIT(target.Context, target.Target, id);
        }

        internal static void TransactionsCommitAsync(IUnmanagedTarget target, long id, long futId)
        {
            TX_COMMIT_ASYNC(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsRollback(IUnmanagedTarget target, long id)
        {
            return TX_ROLLBACK(target.Context, target.Target, id);
        }

        internal static void TransactionsRollbackAsync(IUnmanagedTarget target, long id, long futId)
        {
            TX_ROLLBACK_ASYNC(target.Context, target.Target, id, futId);
        }

        internal static int TransactionsClose(IUnmanagedTarget target, long id)
        {
            return TX_CLOSE(target.Context, target.Target, id);
        }

        internal static int TransactionsState(IUnmanagedTarget target, long id)
        {
            return TX_STATE(target.Context, target.Target, id);
        }

        internal static bool TransactionsSetRollbackOnly(IUnmanagedTarget target, long id)
        {
            return TX_SET_ROLLBACK_ONLY(target.Context, target.Target, id);
        }

        internal static void TransactionsResetMetrics(IUnmanagedTarget target)
        {
            TX_RESET_METRICS(target.Context, target.Target);
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
            char* msgChars = (char*)U.StringToUtf8Unmanaged(e.Message);

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
            var nameChars = (char*)U.StringToUtf8Unmanaged(name);

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
            var nameChars = (char*)U.StringToUtf8Unmanaged(name);

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
            var procPtr = NativeMethods.GetProcAddress(PTR, procName);

            if (procPtr == IntPtr.Zero)
                throw new IgniteException(string.Format("Unable to find native function: {0} (Error code: {1}). " +
                                                      "Make sure that module.def is up to date",
                    procName, Marshal.GetLastWin32Error()));

            return TypeCaster<T>.Cast(Marshal.GetDelegateForFunctionPointer(procPtr, typeof (T)));
        }
    }
}
