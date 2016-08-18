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
    internal static unsafe class IgniteJniNativeMethods
    {
        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteReallocate")]
        public static extern int Reallocate(long memPtr, int cap);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStart")]
        public static extern void* IgnitionStart(void* ctx, sbyte* cfgPath, sbyte* gridName, int factoryId, 
            long dataPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStop")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool IgnitionStop(void* ctx, sbyte* gridName, [MarshalAs(UnmanagedType.U1)] bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteIgnitionStopAll")]
        public static extern void IgnitionStopAll(void* ctx, [MarshalAs(UnmanagedType.U1)] bool cancel);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorReleaseStart")]
        public static extern void ProcessorReleaseStart(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorProjection")]
        public static extern void* ProcessorProjection(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCache")]
        public static extern void* ProcessorCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateCache")]
        public static extern void* ProcessorCreateCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateCacheFromConfig")]
        public static extern void* ProcessorCreateCacheFromConfig(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateCache")]
        public static extern void* ProcessorGetOrCreateCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateCacheFromConfig")]
        public static extern void* ProcessorGetOrCreateCacheFromConfig(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCreateNearCache")]
        public static extern void* ProcessorCreateNearCache(void* ctx, void* obj, sbyte* name, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetOrCreateNearCache")]
        public static extern void* ProcessorGetOrCreateNearCache(void* ctx, void* obj, sbyte* name, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorDestroyCache")]
        public static extern void ProcessorDestroyCache(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAffinity")]
        public static extern void* ProcessorAffinity(void* ctx, void* obj, sbyte* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorDataStreamer")]
        public static extern void* ProcessorDataStreamer(void* ctx, void* obj, sbyte* name, 
            [MarshalAs(UnmanagedType.U1)] bool keepBinary);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorTransactions")]
        public static extern void* ProcessorTransactions(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorCompute")]
        public static extern void* ProcessorCompute(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorMessage")]
        public static extern void* ProcessorMessage(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorEvents")]
        public static extern void* ProcessorEvents(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorServices")]
        public static extern void* ProcessorServices(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorExtensions")]
        public static extern void* ProcessorExtensions(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicLong")]
        public static extern void* ProcessorAtomicLong(void* ctx, void* obj, sbyte* name, long initVal,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicSequence")]
        public static extern void* ProcessorAtomicSequence(void* ctx, void* obj, sbyte* name, long initVal,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorAtomicReference")]
        public static extern void* ProcessorAtomicReference(void* ctx, void* obj, sbyte* name, long memPtr,
            [MarshalAs(UnmanagedType.U1)] bool create);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetIgniteConfiguration")]
        public static extern void ProcessorGetIgniteConfiguration(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProcessorGetCacheNames")]
        public static extern void ProcessorGetCacheNames(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutLong")]
        public static extern long TargetInStreamOutLong(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutStream")]
        public static extern void TargetInStreamOutStream(void* ctx, void* target, int opType, long inMemPtr,
            long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInStreamOutObject")]
        public static extern void* TargetInStreanOutObject(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetInObjectStreamOutStream")]
        public static extern void TargetInObjectStreamOutStream(void* ctx, void* target, int opType,
            void* arg, long inMemPtr, long outMemPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutLong")]
        public static extern long TargetOutLong(void* ctx, void* target, int opType);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutStream")]
        public static extern void TargetOutStream(void* ctx, void* target, int opType, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetOutObject")]
        public static extern void* TargetOutObject(void* ctx, void* target, int opType);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetListenFuture")]
        public static extern void TargetListenFut(void* ctx, void* target, long futId, int typ);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetListenFutureForOperation")]
        public static extern void TargetListenFutForOp(void* ctx, void* target, long futId, int typ, int opId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetListenFutureAndGet")]
        public static extern void* TargetListenFutAndGet(void* ctx, void* target, long futId, int typ);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTargetListenFutureForOperationAndGet")]
        public static extern void* TargetListenFutForOpAndGet(void* ctx, void* target, long futId, int typ, int opId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAffinityPartitions")]
        public static extern int AffinityParts(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithSkipStore")]
        public static extern void* CacheWithSkipStore(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithNoRetries")]
        public static extern void* CacheWithNoRetries(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithExpiryPolicy")]
        public static extern void* CacheWithExpiryPolicy(void* ctx, void* obj, long create, long update, long access);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithAsync")]
        public static extern void* CacheWithAsync(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheWithKeepPortable")]
        public static extern void* CacheWithKeepBinary(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheClear")]
        public static extern void CacheClear(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheRemoveAll")]
        public static extern void CacheRemoveAll(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheOutOpQueryCursor")]
        public static extern void* CacheOutOpQueryCursor(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheOutOpContinuousQuery")]
        public static extern void* CacheOutOpContinuousQuery(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheIterator")]
        public static extern void* CacheIterator(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheLocalIterator")]
        public static extern void* CacheLocalIterator(void* ctx, void* obj, int peekModes);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheEnterLock")]
        public static extern void CacheEnterLock(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheExitLock")]
        public static extern void CacheExitLock(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheTryEnterLock")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool CacheTryEnterLock(void* ctx, void* obj, long id, long timeout);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheCloseLock")]
        public static extern void CacheCloseLock(void* ctx, void* obj, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheRebalance")]
        public static extern void CacheRebalance(void* ctx, void* obj, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheSize")]
        public static extern int CacheSize(void* ctx, void* obj, int peekModes, [MarshalAs(UnmanagedType.U1)] bool loc);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCacheStoreCallbackInvoke")]
        public static extern void CacheStoreCallbackInvoke(void* ctx, void* obj, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeWithNoFailover")]
        public static extern void ComputeWithNoFailover(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeWithTimeout")]
        public static extern void ComputeWithTimeout(void* ctx, void* target, long timeout);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteComputeExecuteNative")]
        public static extern void* ComputeExecuteNative(void* ctx, void* target, long taskPtr, long topVer);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteContinuousQueryClose")]
        public static extern void ContinuousQryClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteContinuousQueryGetInitialQueryCursor")]
        public static extern void* ContinuousQryGetInitialQueryCursor(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerListenTopology")]
        public static extern void DataStreamerListenTop(void* ctx, void* obj, long ptr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerAllowOverwriteGet")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool DataStreamerAllowOverwriteGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerAllowOverwriteSet")]
        public static extern void DataStreamerAllowOverwriteSet(void* ctx, void* obj, 
            [MarshalAs(UnmanagedType.U1)] bool val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerSkipStoreGet")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool DataStreamerSkipStoreGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerSkipStoreSet")]
        public static extern void DataStreamerSkipStoreSet(void* ctx, void* obj, 
            [MarshalAs(UnmanagedType.U1)] bool val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeBufferSizeGet")]
        public static extern int DataStreamerPerNodeBufferSizeGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeBufferSizeSet")]
        public static extern void DataStreamerPerNodeBufferSizeSet(void* ctx, void* obj, int val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeParallelOperationsGet")]
        public static extern int DataStreamerPerNodeParallelOpsGet(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDataStreamerPerNodeParallelOperationsSet")]
        public static extern void DataStreamerPerNodeParallelOpsSet(void* ctx, void* obj, int val);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteMessagingWithAsync")]
        public static extern void* MessagingWithAsync(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForOthers")]
        public static extern void* ProjectionForOthers(void* ctx, void* obj, void* prj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForRemotes")]
        public static extern void* ProjectionForRemotes(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForDaemons")]
        public static extern void* ProjectionForDaemons(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForRandom")]
        public static extern void* ProjectionForRandom(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForOldest")]
        public static extern void* ProjectionForOldest(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForYoungest")]
        public static extern void* ProjectionForYoungest(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionForServers")]
        public static extern void* ProjectionForServers(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionResetMetrics")]
        public static extern void ProjectionResetMetrics(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteProjectionOutOpRet")]
        public static extern void* ProjectionOutOpRet(void* ctx, void* obj, int type, long memPtr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteQueryCursorIterator")]
        public static extern void QryCursorIterator(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteQueryCursorClose")]
        public static extern void QryCursorClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAcquire")]
        public static extern void* Acquire(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteRelease")]
        public static extern void Release(void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsStart")]
        public static extern long TxStart(void* ctx, void* target, int concurrency, int isolation, long timeout,
            int txSize);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsCommit")]
        public static extern int TxCommit(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsCommitAsync")]
        public static extern void TxCommitAsync(void* ctx, void* target, long id, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsRollback")]
        public static extern int TxRollback(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsRollbackAsync")]
        public static extern void TxRollbackAsync(void* ctx, void* target, long id, long futId);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsClose")]
        public static extern int TxClose(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsState")]
        public static extern int TxState(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsSetRollbackOnly")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool TxSetRollbackOnly(void* ctx, void* target, long id);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteTransactionsResetMetrics")]
        public static extern void TxResetMetrics(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteThrowToJava")]
        public static extern void ThrowToJava(void* ctx, char* msg);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteHandlersSize")]
        public static extern int HandlersSize();

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteCreateContext")]
        public static extern void* CreateContext(void* opts, int optsLen, void* cbs);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDeleteContext")]
        public static extern void DeleteContext(void* ptr);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteDestroyJvm")]
        public static extern void DestroyJvm(void* ctx);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsWithAsync")]
        public static extern void* EventsWithAsync(void* ctx, void* obj);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsStopLocalListen")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool EventsStopLocalListen(void* ctx, void* obj, long hnd);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsLocalListen")]
        public static extern void EventsLocalListen(void* ctx, void* obj, long hnd, int type);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteEventsIsEnabled")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool EventsIsEnabled(void* ctx, void* obj, int type);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesWithAsync")]
        public static extern void* ServicesWithAsync(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesWithServerKeepPortable")]
        public static extern void* ServicesWithServerKeepBinary(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesCancel")]
        public static extern long ServicesCancel(void* ctx, void* target, char* name);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesCancelAll")]
        public static extern long ServicesCancelAll(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteServicesGetServiceProxy")]
        public static extern void* ServicesGetServiceProxy(void* ctx, void* target, char* name,
            [MarshalAs(UnmanagedType.U1)] bool sticky);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongGet")]
        public static extern long AtomicLongGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongIncrementAndGet")]
        public static extern long AtomicLongIncrementAndGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongAddAndGet")]
        public static extern long AtomicLongAddAndGet(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongDecrementAndGet")]
        public static extern long AtomicLongDecrementAndGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongGetAndSet")]
        public static extern long AtomicLongGetAndSet(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongCompareAndSetAndGet")]
        public static extern long AtomicLongCompareAndSetAndGet(void* ctx, void* target, long expVal, long newVal);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongIsClosed")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool AtomicLongIsClosed(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicLongClose")]
        public static extern void AtomicLongClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceGet")]
        public static extern long AtomicSequenceGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceIncrementAndGet")]
        public static extern long AtomicSequenceIncrementAndGet(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceAddAndGet")]
        public static extern long AtomicSequenceAddAndGet(void* ctx, void* target, long value);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceGetBatchSize")]
        public static extern int AtomicSequenceGetBatchSize(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceSetBatchSize")]
        public static extern void AtomicSequenceSetBatchSize(void* ctx, void* target, int size);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceIsClosed")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool AtomicSequenceIsClosed(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicSequenceClose")]
        public static extern void AtomicSequenceClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicReferenceIsClosed")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool AtomicReferenceIsClosed(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteAtomicReferenceClose")]
        public static extern void AtomicReferenceClose(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteListenableCancel")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool ListenableCancel(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteListenableIsCancelled")]
        [return: MarshalAs(UnmanagedType.U1)]
        public static extern bool ListenableIsCancelled(void* ctx, void* target);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteSetConsoleHandler")]
        public static extern void SetConsoleHandler(void* consoleHandler);

        [DllImport(IgniteUtils.FileIgniteJniDll, EntryPoint = "IgniteRemoveConsoleHandler")]
        public static extern int RemoveConsoleHandler(void* consoleHandler);
    }
}