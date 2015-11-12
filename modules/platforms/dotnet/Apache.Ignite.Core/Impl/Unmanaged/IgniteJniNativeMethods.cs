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
    using System.Security;

    /// <summary>
    /// Ignite JNI native methods.
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal unsafe static class IgniteJniNativeMethods
    {
        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteReallocate", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int Reallocate(long memPtr, int cap);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStart", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* IgnitionStart(void* ctx, sbyte* cfgPath, sbyte* gridName, int factoryId,
            long dataPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStop", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool IgnitionStop(void* ctx, sbyte* gridName, [MarshalAs(UnmanagedType.U1)] bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStopAll", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void IgnitionStopAll(void* ctx, [MarshalAs(UnmanagedType.U1)] bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorReleaseStart", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ProcessorReleaseStart(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorProjection", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorProjection(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCache", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateCache", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorCreateCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateCache",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorGetOrCreateCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAffinity", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorAffinity(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorDataStreamer", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorDataStreamer(void* ctx, void* obj, sbyte* name, 
            [MarshalAs(UnmanagedType.U1)] bool keepBinary);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorTransactions", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorTransactions(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCompute", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorCompute(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorMessage", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorMessage(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorEvents", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorEvents(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorServices", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorServices(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorExtensions", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorExtensions(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicLong", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProcessorAtomicLong(void* ctx, void* obj, sbyte* name, long initVal,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutLong", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long TargetInStreamOutLong(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutStream", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TargetInStreamOutStream(void* ctx, void* target, int opType, long inMemPtr,
            long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutObject", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* TargetInStreanOutObject(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInObjectStreamOutStream",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TargetInObjectStreamOutStream(void* ctx, void* target, int opType,
            void* arg, long inMemPtr, long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutLong", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long TargetOutLong(void* ctx, void* target, int opType);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutStream", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TargetOutStream(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutObject", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* TargetOutObject(void* ctx, void* target, int opType);

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
        public static extern void* CacheWithSkipStore(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithNoRetries", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheWithNoRetries(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithExpiryPolicy", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheWithExpiryPolicy(void* ctx, void* obj, long create, long update,
            long access);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheWithAsync(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithKeepPortable", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheWithKeepBinary(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheClear", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheClear(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheRemoveAll", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheRemoveAll(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheOutOpQueryCursor", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheOutOpQueryCursor(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheOutOpContinuousQuery",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheOutOpContinuousQuery(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheIterator", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheIterator(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheLocalIterator", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CacheLocalIterator(void* ctx, void* obj, int peekModes);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheEnterLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheEnterLock(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheExitLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheExitLock(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheTryEnterLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool CacheTryEnterLock(void* ctx, void* obj, long id, long timeout);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheCloseLock", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheCloseLock(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheRebalance", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheRebalance(void* ctx, void* obj, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheSize", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int CacheSize(void* ctx, void* obj, int peekModes, [MarshalAs(UnmanagedType.U1)] bool loc);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheStoreCallbackInvoke",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void CacheStoreCallbackInvoke(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeWithNoFailover", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ComputeWithNoFailover(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeWithTimeout", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ComputeWithTimeout(void* ctx, void* target, long timeout);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeExecuteNative", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ComputeExecuteNative(void* ctx, void* target, long taskPtr, long topVer);

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
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool DataStreamerAllowOverwriteGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerAllowOverwriteSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DataStreamerAllowOverwriteSet(void* ctx, void* obj, 
            [MarshalAs(UnmanagedType.U1)] bool val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerSkipStoreGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool DataStreamerSkipStoreGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerSkipStoreSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DataStreamerSkipStoreSet(void* ctx, void* obj, 
            [MarshalAs(UnmanagedType.U1)] bool val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeBufferSizeGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int DataStreamerPerNodeBufferSizeGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeBufferSizeSet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DataStreamerPerNodeBufferSizeSet(void* ctx, void* obj, int val);

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
        public static extern void* ProjectionForOthers(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForRemotes", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProjectionForRemotes(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForDaemons", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProjectionForDaemons(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForRandom", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProjectionForRandom(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForOldest", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProjectionForOldest(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForYoungest", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProjectionForYoungest(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionResetMetrics", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ProjectionResetMetrics(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionOutOpRet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ProjectionOutOpRet(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteQueryCursorIterator", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void QryCursorIterator(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteQueryCursorClose", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void QryCursorClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAcquire", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* Acquire(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteRelease", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void Release(void* target);

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
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool TxSetRollbackOnly(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsResetMetrics",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void TxResetMetrics(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteThrowToJava", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void ThrowToJava(void* ctx, char* msg);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteHandlersSize", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern int HandlersSize();

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCreateContext", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* CreateContext(void* opts, int optsLen, void* cbs);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDeleteContext", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DeleteContext(void* ptr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDestroyJvm", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void DestroyJvm(void* ctx);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* EventsWithAsync(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsStopLocalListen", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool EventsStopLocalListen(void* ctx, void* obj, long hnd);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsLocalListen", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void EventsLocalListen(void* ctx, void* obj, long hnd, int type);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsIsEnabled", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool EventsIsEnabled(void* ctx, void* obj, int type);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesWithAsync", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ServicesWithAsync(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesWithServerKeepPortable",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ServicesWithServerKeepBinary(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesCancel", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ServicesCancel(void* ctx, void* target, char* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesCancelAll", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long ServicesCancelAll(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesGetServiceProxy", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void* ServicesGetServiceProxy(void* ctx, void* target, char* name,
            [MarshalAs(UnmanagedType.U1)] bool sticky);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongGet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long AtomicLongGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongIncrementAndGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long AtomicLongIncrementAndGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongAddAndGet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long AtomicLongAddAndGet(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongDecrementAndGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long AtomicLongDecrementAndGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongGetAndSet", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long AtomicLongGetAndSet(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongCompareAndSetAndGet",
            SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern long AtomicLongCompareAndSetAndGet(void* ctx, void* target, long expVal,
            long newVal);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongIsClosed", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool AtomicLongIsClosed(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongClose", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        public static extern void AtomicLongClose(void* ctx, void* target);
    }
}