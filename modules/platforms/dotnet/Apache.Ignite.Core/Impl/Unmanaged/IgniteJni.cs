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
    using System.Runtime.InteropServices;

    /// <summary>
    /// Ignite JNI methods.
    /// </summary>
    internal unsafe static class IgniteJni
    {
        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteReallocate", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int REALLOCATE(long memPtr, int cap);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStart", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* IGNITION_START(void* ctx, sbyte* cfgPath, sbyte* gridName, int factoryId,
            long dataPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStop", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool IGNITION_STOP(void* ctx, sbyte* gridName, bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStopAll", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void IGNITION_STOP_ALL(void* ctx, bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorReleaseStart", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void PROCESSOR_RELEASE_START(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorProjection", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_PROJECTION(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCache", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_CACHE(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateCache", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_CREATE_CACHE(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateCache",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_GET_OR_CREATE_CACHE(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAffinity", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_AFFINITY(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorDataStreamer", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_DATA_STREAMER(void* ctx, void* obj, sbyte* name, bool keepBinary);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorTransactions", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_TRANSACTIONS(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCompute", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_COMPUTE(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorMessage", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_MESSAGE(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorEvents", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_EVENTS(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorServices", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_SERVICES(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorExtensions", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_EXTENSIONS(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicLong", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROCESSOR_ATOMIC_LONG(void* ctx, void* obj, sbyte* name, long initVal,
            bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutLong", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long TARGET_IN_STREAM_OUT_LONG(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutStream", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TARGET_IN_STREAM_OUT_STREAM(void* ctx, void* target, int opType, long inMemPtr,
            long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutObject", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* TARGET_IN_STREAM_OUT_OBJECT(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInObjectStreamOutStream",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TARGET_IN_OBJECT_STREAM_OUT_STREAM(void* ctx, void* target, int opType,
            void* arg, long inMemPtr, long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutLong", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long TARGET_OUT_LONG(void* ctx, void* target, int opType);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutStream", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TARGET_OUT_STREAM(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutObject", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* TARGET_OUT_OBJECT(void* ctx, void* target, int opType);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetListenFuture", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TargetListenFut(void* ctx, void* target, long futId, int typ);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetListenFutureForOperation",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TargetListenFutForOp(void* ctx, void* target, long futId, int typ, int opId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAffinityPartitions", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int AffinityParts(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithSkipStore", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_WITH_SKIP_STORE(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithNoRetries", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_WITH_NO_RETRIES(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithExpiryPolicy", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_WITH_EXPIRY_POLICY(void* ctx, void* obj, long create, long update,
            long access);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_WITH_ASYNC(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithKeepPortable", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_WITH_KEEP_BINARY(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheClear", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_CLEAR(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheRemoveAll", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_REMOVE_ALL(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheOutOpQueryCursor", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_OUT_OP_QUERY_CURSOR(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheOutOpContinuousQuery",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_OUT_OP_CONTINUOUS_QUERY(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheIterator", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_ITERATOR(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheLocalIterator", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CACHE_LOCAL_ITERATOR(void* ctx, void* obj, int peekModes);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheEnterLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_ENTER_LOCK(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheExitLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_EXIT_LOCK(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheTryEnterLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool CACHE_TRY_ENTER_LOCK(void* ctx, void* obj, long id, long timeout);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheCloseLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_CLOSE_LOCK(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheRebalance", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_REBALANCE(void* ctx, void* obj, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheSize", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int CACHE_SIZE(void* ctx, void* obj, int peekModes, bool loc);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheStoreCallbackInvoke",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CACHE_STORE_CALLBACK_INVOKE(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeWithNoFailover", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void COMPUTE_WITH_NO_FAILOVER(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeWithTimeout", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void COMPUTE_WITH_TIMEOUT(void* ctx, void* target, long timeout);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeExecuteNative", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void COMPUTE_EXECUTE_NATIVE(void* ctx, void* target, long taskPtr, long topVer);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteContinuousQueryClose", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ContinuousQryClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteContinuousQueryGetInitialQueryCursor",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ContinuousQryGetInitialQueryCursor(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerListenTopology",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DataStreamerListenTop(void* ctx, void* obj, long ptr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerAllowOverwriteGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool DATA_STREAMER_ALLOW_OVERWRITE_GET(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerAllowOverwriteSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DATA_STREAMER_ALLOW_OVERWRITE_SET(void* ctx, void* obj, bool val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerSkipStoreGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool DATA_STREAMER_SKIP_STORE_GET(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerSkipStoreSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DATA_STREAMER_SKIP_STORE_SET(void* ctx, void* obj, bool val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeBufferSizeGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int DATA_STREAMER_PER_NODE_BUFFER_SIZE_GET(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeBufferSizeSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DATA_STREAMER_PER_NODE_BUFFER_SIZE_SET(void* ctx, void* obj, int val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeParallelOperationsGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int DataStreamerPerNodeParallelOpsGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeParallelOperationsSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DataStreamerPerNodeParallelOpsSet(void* ctx, void* obj, int val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteMessagingWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* MessagingWithAsync(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForOthers", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_FOR_OTHERS(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForRemotes", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_FOR_REMOTES(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForDaemons", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_FOR_DAEMONS(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForRandom", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_FOR_RANDOM(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForOldest", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_FOR_OLDEST(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForYoungest", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_FOR_YOUNGEST(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionResetMetrics", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void PROJECTION_RESET_METRICS(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionOutOpRet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* PROJECTION_OUT_OP_RET(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteQueryCursorIterator", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void QryCursorIterator(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteQueryCursorClose", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void QryCursorClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAcquire", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ACQUIRE(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteRelease", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void RELEASE(void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsStart", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long TxStart(void* ctx, void* target, int concurrency, int isolation, long timeout,
            int txSize);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsCommit", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int TxCommit(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsCommitAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TxCommitAsync(void* ctx, void* target, long id, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsRollback", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int TxRollback(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsRollbackAsync",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TxRollbackAsync(void* ctx, void* target, long id, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsClose", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int TxClose(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsState", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int TxState(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsSetRollbackOnly",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool TxSetRollbackOnly(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsResetMetrics",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TxResetMetrics(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteThrowToJava", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void THROW_TO_JAVA(void* ctx, char* msg);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteHandlersSize", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int HANDLERS_SIZE();

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCreateContext", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CREATE_CONTEXT(void* opts, int optsLen, void* cbs);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDeleteContext", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DELETE_CONTEXT(void* ptr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDestroyJvm", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DESTROY_JVM(void* ctx);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* EVENTS_WITH_ASYNC(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsStopLocalListen", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool EVENTS_STOP_LOCAL_LISTEN(void* ctx, void* obj, long hnd);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsLocalListen", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void EVENTS_LOCAL_LISTEN(void* ctx, void* obj, long hnd, int type);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsIsEnabled", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool EVENTS_IS_ENABLED(void* ctx, void* obj, int type);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* SERVICES_WITH_ASYNC(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesWithServerKeepPortable",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* SERVICES_WITH_SERVER_KEEP_BINARY(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesCancel", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long SERVICES_CANCEL(void* ctx, void* target, char* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesCancelAll", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long SERVICES_CANCEL_ALL(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesGetServiceProxy", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* SERVICES_GET_SERVICE_PROXY(void* ctx, void* target, char* name, bool sticky);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongGet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ATOMIC_LONG_GET(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongIncrementAndGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ATOMIC_LONG_INCREMENT_AND_GET(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongAddAndGet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ATOMIC_LONG_ADD_AND_GET(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongDecrementAndGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ATOMIC_LONG_DECREMENT_AND_GET(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongGetAndSet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ATOMIC_LONG_GET_AND_SET(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongCompareAndSetAndGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ATOMIC_LONG_COMPARE_AND_SET_AND_GET(void* ctx, void* target, long expVal,
            long newVal);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongIsClosed", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern bool ATOMIC_LONG_IS_CLOSED(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongClose", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ATOMIC_LONG_CLOSE(void* ctx, void* target);
    }
}