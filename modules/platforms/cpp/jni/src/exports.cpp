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

#include "ignite/jni/exports.h"
#include "ignite/jni/java.h"

namespace gcj = ignite::jni::java;

/* --- Target methods. --- */
extern "C" {
    int IGNITE_CALL IgniteReallocate(long long memPtr, int cap) {
        return gcj::JniContext::Reallocate(memPtr, cap);
    }

    void* IGNITE_CALL IgniteIgnitionStart(gcj::JniContext* ctx, char* cfgPath, char* name, int factoryId, long long dataPtr) {
        return ctx->IgnitionStart(cfgPath, name, factoryId, dataPtr);
    }

	void* IGNITE_CALL IgniteIgnitionInstance(gcj::JniContext* ctx, char* name) {
        return ctx->IgnitionInstance(name);
    }

    long long IGNITE_CALL IgniteIgnitionEnvironmentPointer(gcj::JniContext* ctx, char* name) {
        return ctx->IgnitionEnvironmentPointer(name);
    }

	bool IGNITE_CALL IgniteIgnitionStop(gcj::JniContext* ctx, char* name, bool cancel) {
        return ctx->IgnitionStop(name, cancel);
    }

	void IGNITE_CALL IgniteIgnitionStopAll(gcj::JniContext* ctx, bool cancel) {
        return ctx->IgnitionStopAll(cancel);
    }

    void IGNITE_CALL IgniteProcessorReleaseStart(gcj::JniContext* ctx, void* obj) {
        return ctx->ProcessorReleaseStart(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProcessorProjection(gcj::JniContext* ctx, void* obj) {
        return ctx->ProcessorProjection(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProcessorCache(gcj::JniContext* ctx, void* obj, char* name) {
        return ctx->ProcessorCache(static_cast<jobject>(obj), name);
    }

    void* IGNITE_CALL IgniteProcessorCreateCache(gcj::JniContext* ctx, void* obj, char* name) {
        return ctx->ProcessorCreateCache(static_cast<jobject>(obj), name);
    }

    void* IGNITE_CALL IgniteProcessorGetOrCreateCache(gcj::JniContext* ctx, void* obj, char* name) {
        return ctx->ProcessorGetOrCreateCache(static_cast<jobject>(obj), name);
    }

    void* IGNITE_CALL IgniteProcessorCreateCacheFromConfig(gcj::JniContext* ctx, void* obj, long long memPtr) {
        return ctx->ProcessorCreateCacheFromConfig(static_cast<jobject>(obj), memPtr);
    }

    void* IGNITE_CALL IgniteProcessorGetOrCreateCacheFromConfig(gcj::JniContext* ctx, void* obj, long long memPtr) {
        return ctx->ProcessorGetOrCreateCacheFromConfig(static_cast<jobject>(obj), memPtr);
    }

    void* IGNITE_CALL IgniteProcessorCreateNearCache(gcj::JniContext* ctx, void* obj, char* name, long long memPtr) {
        return ctx->ProcessorCreateNearCache(static_cast<jobject>(obj), name, memPtr);
    }

    void* IGNITE_CALL IgniteProcessorGetOrCreateNearCache(gcj::JniContext* ctx, void* obj, char* name, long long memPtr) {
        return ctx->ProcessorGetOrCreateNearCache(static_cast<jobject>(obj), name, memPtr);
    }

    void IGNITE_CALL IgniteProcessorDestroyCache(gcj::JniContext* ctx, void* obj, char* name) {
        ctx->ProcessorDestroyCache(static_cast<jobject>(obj), name);
    }

    void* IGNITE_CALL IgniteProcessorAffinity(gcj::JniContext* ctx, void* obj, char* name) {
        return ctx->ProcessorAffinity(static_cast<jobject>(obj), name);
    }

    void*IGNITE_CALL IgniteProcessorDataStreamer(gcj::JniContext* ctx, void* obj, char* name, bool keepPortable) {
        return ctx->ProcessorDataStreamer(static_cast<jobject>(obj), name, keepPortable);
    }
    
    void* IGNITE_CALL IgniteProcessorTransactions(gcj::JniContext* ctx, void* obj) {
        return ctx->ProcessorTransactions(static_cast<jobject>(obj));
    }
        
    void* IGNITE_CALL IgniteProcessorCompute(gcj::JniContext* ctx, void* obj, void* prj) {
        return ctx->ProcessorCompute(static_cast<jobject>(obj), static_cast<jobject>(prj));
    }

    void* IGNITE_CALL IgniteProcessorMessage(gcj::JniContext* ctx, void* obj, void* prj) {
        return ctx->ProcessorMessage(static_cast<jobject>(obj), static_cast<jobject>(prj));
    }

    void* IGNITE_CALL IgniteProcessorEvents(gcj::JniContext* ctx, void* obj, void* prj) {
        return ctx->ProcessorEvents(static_cast<jobject>(obj), static_cast<jobject>(prj));
    }

    void* IGNITE_CALL IgniteProcessorServices(gcj::JniContext* ctx, void* obj, void* prj) {
        return ctx->ProcessorServices(static_cast<jobject>(obj), static_cast<jobject>(prj));
    }

    void* IGNITE_CALL IgniteProcessorExtensions(gcj::JniContext* ctx, void* obj) {
        return ctx->ProcessorExtensions(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProcessorAtomicLong(gcj::JniContext* ctx, void* obj, char* name, long long initVal, bool create) {
        return ctx->ProcessorAtomicLong(static_cast<jobject>(obj), name, initVal, create);
    }

    void* IGNITE_CALL IgniteProcessorAtomicSequence(gcj::JniContext* ctx, void* obj, char* name, long long initVal, bool create) {
        return ctx->ProcessorAtomicSequence(static_cast<jobject>(obj), name, initVal, create);
    }

    void* IGNITE_CALL IgniteProcessorAtomicReference(gcj::JniContext* ctx, void* obj, char* name, long long memPtr, bool create) {
        return ctx->ProcessorAtomicReference(static_cast<jobject>(obj), name, memPtr, create);
    }

	void IGNITE_CALL IgniteProcessorGetIgniteConfiguration(gcj::JniContext* ctx, void* obj, long long memPtr) {
        return ctx->ProcessorGetIgniteConfiguration(static_cast<jobject>(obj), memPtr);
    }

	void IGNITE_CALL IgniteProcessorGetCacheNames(gcj::JniContext* ctx, void* obj, long long memPtr) {
        return ctx->ProcessorGetCacheNames(static_cast<jobject>(obj), memPtr);
    }

    long long IGNITE_CALL IgniteTargetInStreamOutLong(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        return ctx->TargetInStreamOutLong(static_cast<jobject>(obj), opType, memPtr);
    }

    void IGNITE_CALL IgniteTargetInStreamOutStream(gcj::JniContext* ctx, void* obj, int opType, long long inMemPtr, long long outMemPtr) {
        ctx->TargetInStreamOutStream(static_cast<jobject>(obj), opType, inMemPtr, outMemPtr);
    }

    void* IGNITE_CALL IgniteTargetInStreamOutObject(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        return ctx->TargetInStreamOutObject(static_cast<jobject>(obj), opType, memPtr);
    }

    void IGNITE_CALL IgniteTargetInObjectStreamOutStream(gcj::JniContext* ctx, void* obj, int opType, void* arg, long long inMemPtr, long long outMemPtr) {
        ctx->TargetInObjectStreamOutStream(static_cast<jobject>(obj), opType, arg, inMemPtr, outMemPtr);
    }
    
    long long IGNITE_CALL IgniteTargetOutLong(gcj::JniContext* ctx, void* obj, int opType) {
        return ctx->TargetOutLong(static_cast<jobject>(obj), opType);
    }

    void IGNITE_CALL IgniteTargetOutStream(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        ctx->TargetOutStream(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetOutObject(gcj::JniContext* ctx, void* obj, int opType) {
        return ctx->TargetOutObject(static_cast<jobject>(obj), opType);
    }

    void IGNITE_CALL IgniteTargetListenFuture(gcj::JniContext* ctx, void* obj, long long futId, int typ) {
        ctx->TargetListenFuture(static_cast<jobject>(obj), futId, typ);
    }

    void IGNITE_CALL IgniteTargetListenFutureForOperation(gcj::JniContext* ctx, void* obj, long long futId, int typ, int opId) {
        ctx->TargetListenFutureForOperation(static_cast<jobject>(obj), futId, typ, opId);
    }

    void* IGNITE_CALL IgniteTargetListenFutureAndGet(gcj::JniContext* ctx, void* obj, long long futId, int typ) {
        return ctx->TargetListenFutureAndGet(static_cast<jobject>(obj), futId, typ);
    }

    void* IGNITE_CALL IgniteTargetListenFutureForOperationAndGet(gcj::JniContext* ctx, void* obj, long long futId, int typ, int opId) {
        return ctx->TargetListenFutureForOperationAndGet(static_cast<jobject>(obj), futId, typ, opId);
    }

    int IGNITE_CALL IgniteAffinityPartitions(gcj::JniContext* ctx, void* obj) {
        return ctx->AffinityPartitions(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteCacheWithSkipStore(gcj::JniContext* ctx, void* obj) {
        return ctx->CacheWithSkipStore(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteCacheWithNoRetries(gcj::JniContext* ctx, void* obj) {
        return ctx->CacheWithNoRetries(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteCacheWithExpiryPolicy(gcj::JniContext* ctx, void* obj, long long create, long long update, long long access) {
        return ctx->CacheWithExpiryPolicy(static_cast<jobject>(obj), create, update, access);
    }

    void* IGNITE_CALL IgniteCacheWithAsync(gcj::JniContext* ctx, void* obj) {
        return ctx->CacheWithAsync(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteCacheWithKeepPortable(gcj::JniContext* ctx, void* obj)
    {
        return ctx->CacheWithKeepPortable(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteCacheClear(gcj::JniContext* ctx, void* obj) {
        ctx->CacheClear(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteCacheRemoveAll(gcj::JniContext* ctx, void* obj) {
        ctx->CacheRemoveAll(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteCacheOutOpQueryCursor(gcj::JniContext* ctx, void* obj, int type, long long memPtr) {
        return ctx->CacheOutOpQueryCursor(static_cast<jobject>(obj), type, memPtr);
    }

    void* IGNITE_CALL IgniteCacheOutOpContinuousQuery(gcj::JniContext* ctx, void* obj, int type, long long memPtr) {
        return ctx->CacheOutOpContinuousQuery(static_cast<jobject>(obj), type, memPtr);
    }

    void* IGNITE_CALL IgniteCacheIterator(gcj::JniContext* ctx, void* obj) {
        return ctx->CacheIterator(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteCacheLocalIterator(gcj::JniContext* ctx, void* obj, int peekModes) {
        return ctx->CacheLocalIterator(static_cast<jobject>(obj), peekModes);
    }

    void IGNITE_CALL IgniteCacheEnterLock(gcj::JniContext* ctx, void* obj, long long id) {
        ctx->CacheEnterLock(static_cast<jobject>(obj), id);
    }

    void IGNITE_CALL IgniteCacheExitLock(gcj::JniContext* ctx, void* obj, long long id) {
        ctx->CacheExitLock(static_cast<jobject>(obj), id);
    }

    bool IGNITE_CALL IgniteCacheTryEnterLock(gcj::JniContext* ctx, void* obj, long long id, long long timeout) {
        return ctx->CacheTryEnterLock(static_cast<jobject>(obj), id, timeout);
    }

    void IGNITE_CALL IgniteCacheCloseLock(gcj::JniContext* ctx, void* obj, long long id) {
        ctx->CacheCloseLock(static_cast<jobject>(obj), id);
    }

    void IGNITE_CALL IgniteCacheRebalance(gcj::JniContext* ctx, void* obj, long long futId) {
        ctx->CacheRebalance(static_cast<jobject>(obj), futId);
    }

    int IGNITE_CALL IgniteCacheSize(gcj::JniContext* ctx, void* obj, int peekModes, bool loc) {
        return ctx->CacheSize(static_cast<jobject>(obj), peekModes, loc);
    }

    void IGNITE_CALL IgniteComputeWithNoFailover(gcj::JniContext* ctx, void* obj) {
        ctx->ComputeWithNoFailover(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteComputeWithTimeout(gcj::JniContext* ctx, void* obj, long long timeout) {
        ctx->ComputeWithTimeout(static_cast<jobject>(obj), timeout);
    }

    void* IGNITE_CALL IgniteComputeExecuteNative(gcj::JniContext* ctx, void* obj, long long taskPtr, long long topVer) {
        return ctx->ComputeExecuteNative(static_cast<jobject>(obj), taskPtr, topVer);
    }

    void IGNITE_CALL IgniteContinuousQueryClose(gcj::JniContext* ctx, void* obj) {
        ctx->ContinuousQueryClose(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteContinuousQueryGetInitialQueryCursor(gcj::JniContext* ctx, void* obj) {
        return ctx->ContinuousQueryGetInitialQueryCursor(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteCacheStoreCallbackInvoke(gcj::JniContext* ctx, void* obj, long long memPtr) {
        ctx->CacheStoreCallbackInvoke(static_cast<jobject>(obj), memPtr);
    }

    void IGNITE_CALL IgniteDataStreamerListenTopology(gcj::JniContext* ctx, void* obj, long long ptr) {
        ctx->DataStreamerListenTopology(static_cast<jobject>(obj), ptr);
    }

    bool IGNITE_CALL IgniteDataStreamerAllowOverwriteGet(gcj::JniContext* ctx, void* obj) {
        return ctx->DataStreamerAllowOverwriteGet(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteDataStreamerAllowOverwriteSet(gcj::JniContext* ctx, void* obj, bool val) {
        ctx->DataStreamerAllowOverwriteSet(static_cast<jobject>(obj), val);
    }

    bool IGNITE_CALL IgniteDataStreamerSkipStoreGet(gcj::JniContext* ctx, void* obj) {
        return ctx->DataStreamerSkipStoreGet(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteDataStreamerSkipStoreSet(gcj::JniContext* ctx, void* obj, bool val) {
        ctx->DataStreamerSkipStoreSet(static_cast<jobject>(obj), val);
    }

    int IGNITE_CALL IgniteDataStreamerPerNodeBufferSizeGet(gcj::JniContext* ctx, void* obj) {
        return ctx->DataStreamerPerNodeBufferSizeGet(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteDataStreamerPerNodeBufferSizeSet(gcj::JniContext* ctx, void* obj, int val) {
        ctx->DataStreamerPerNodeBufferSizeSet(static_cast<jobject>(obj), val);
    }

    int IGNITE_CALL IgniteDataStreamerPerNodeParallelOperationsGet(gcj::JniContext* ctx, void* obj) {
        return ctx->DataStreamerPerNodeParallelOperationsGet(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteDataStreamerPerNodeParallelOperationsSet(gcj::JniContext* ctx, void* obj, int val) {
        ctx->DataStreamerPerNodeParallelOperationsSet(static_cast<jobject>(obj), val);
    }

    void* IGNITE_CALL IgniteMessagingWithAsync(gcj::JniContext* ctx, void* obj) {
        return ctx->MessagingWithAsync(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionForOthers(gcj::JniContext* ctx, void* obj, void* prj) {
        return ctx->ProjectionForOthers(static_cast<jobject>(obj), static_cast<jobject>(prj));
    }

    void* IGNITE_CALL IgniteProjectionForRemotes(gcj::JniContext* ctx, void* obj) {
        return ctx->ProjectionForRemotes(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionForDaemons(gcj::JniContext* ctx, void* obj) {
        return ctx->ProjectionForDaemons(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionForRandom(gcj::JniContext* ctx, void* obj) {
        return ctx->ProjectionForRandom(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionForOldest(gcj::JniContext* ctx, void* obj) {
        return ctx->ProjectionForOldest(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionForYoungest(gcj::JniContext* ctx, void* obj) {
        return ctx->ProjectionForYoungest(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionForServers(gcj::JniContext* ctx, void* obj) {
        return ctx->ProjectionForServers(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteProjectionResetMetrics(gcj::JniContext* ctx, void* obj) {
        ctx->ProjectionResetMetrics(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteProjectionOutOpRet(gcj::JniContext* ctx, void* obj, int type, long long memPtr) {
        return ctx->ProjectionOutOpRet(static_cast<jobject>(obj), type, memPtr);
    }

    void IGNITE_CALL IgniteQueryCursorIterator(gcj::JniContext* ctx, void* obj) {
        ctx->QueryCursorIterator(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteQueryCursorClose(gcj::JniContext* ctx, void* obj) {
        ctx->QueryCursorClose(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteTransactionsStart(gcj::JniContext* ctx, void* obj, int concurrency, int isolation, long long timeout, int txSize) {
        return ctx->TransactionsStart(static_cast<jobject>(obj), concurrency, isolation, timeout, txSize);
    }   

    int IGNITE_CALL IgniteTransactionsCommit(gcj::JniContext* ctx, void* obj, long long id) {
        return ctx->TransactionsCommit(static_cast<jobject>(obj), id);
    }

    void IGNITE_CALL IgniteTransactionsCommitAsync(gcj::JniContext* ctx, void* obj, long long id, long long futId) {
        return ctx->TransactionsCommitAsync(static_cast<jobject>(obj), id, futId);
    }

    int IGNITE_CALL IgniteTransactionsRollback(gcj::JniContext* ctx, void* obj, long long id) {
        return ctx->TransactionsRollback(static_cast<jobject>(obj), id);
    }

    void IGNITE_CALL IgniteTransactionsRollbackAsync(gcj::JniContext* ctx, void* obj, long long id, long long futId) {
        return ctx->TransactionsRollbackAsync(static_cast<jobject>(obj), id, futId);
    }

    int IGNITE_CALL IgniteTransactionsClose(gcj::JniContext* ctx, void* obj, long long id) {
        return ctx->TransactionsClose(static_cast<jobject>(obj), id);
    }

    int IGNITE_CALL IgniteTransactionsState(gcj::JniContext* ctx, void* obj, long long id) {
        return ctx->TransactionsState(static_cast<jobject>(obj), id);
    }

    bool IGNITE_CALL IgniteTransactionsSetRollbackOnly(gcj::JniContext* ctx, void* obj, long long id) {
        return ctx->TransactionsSetRollbackOnly(static_cast<jobject>(obj), id);
    }

    void IGNITE_CALL IgniteTransactionsResetMetrics(gcj::JniContext* ctx, void* obj) {
        ctx->TransactionsResetMetrics(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteAcquire(gcj::JniContext* ctx, void* obj) {
        return ctx->Acquire(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteRelease(void* obj) {
        gcj::JniContext::Release(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteThrowToJava(gcj::JniContext* ctx, char* err) {
        ctx->ThrowToJava(err);
    }
    
    int IGNITE_CALL IgniteHandlersSize() {
        return sizeof(gcj::JniHandlers);
    }

    void* IGNITE_CALL IgniteCreateContext(char** opts, int optsLen, gcj::JniHandlers* cbs) {
        return gcj::JniContext::Create(opts, optsLen, *cbs);
    }

    void IGNITE_CALL IgniteDeleteContext(gcj::JniContext* ctx) {
        delete ctx;
    }

    void IGNITE_CALL IgniteDestroyJvm(gcj::JniContext* ctx) {
        ctx->DestroyJvm();
    }

    void* IGNITE_CALL IgniteEventsWithAsync(gcj::JniContext* ctx, void* obj) {
        return ctx->EventsWithAsync(static_cast<jobject>(obj));
    }

    bool IGNITE_CALL IgniteEventsStopLocalListen(gcj::JniContext* ctx, void* obj, long long hnd) {
        return ctx->EventsStopLocalListen(static_cast<jobject>(obj), hnd);
    }

    void IGNITE_CALL IgniteEventsLocalListen(gcj::JniContext* ctx, void* obj, long long hnd, int type) {
        ctx->EventsLocalListen(static_cast<jobject>(obj), hnd, type);
    }

    bool IGNITE_CALL IgniteEventsIsEnabled(gcj::JniContext* ctx, void* obj, int type) {
        return ctx->EventsIsEnabled(static_cast<jobject>(obj), type);
    }    
    
	void* IGNITE_CALL IgniteServicesWithAsync(gcj::JniContext* ctx, void* obj) {
		return ctx->ServicesWithAsync(static_cast<jobject>(obj));
    }

    void* IGNITE_CALL IgniteServicesWithServerKeepPortable(gcj::JniContext* ctx, void* obj) {
        return ctx->ServicesWithServerKeepPortable(static_cast<jobject>(obj));
    }

	void IGNITE_CALL IgniteServicesCancel(gcj::JniContext* ctx, void* obj, char* name) {
		ctx->ServicesCancel(static_cast<jobject>(obj), name);
    }

	void IGNITE_CALL IgniteServicesCancelAll(gcj::JniContext* ctx, void* obj) {
		ctx->ServicesCancelAll(static_cast<jobject>(obj));
    }

	void* IGNITE_CALL IgniteServicesGetServiceProxy(gcj::JniContext* ctx, void* obj, char* name, bool sticky) {
		return ctx->ServicesGetServiceProxy(static_cast<jobject>(obj), name, sticky);
    }

    long long IGNITE_CALL IgniteAtomicLongGet(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongGet(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicLongIncrementAndGet(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongIncrementAndGet(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicLongGetAndIncrement(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongGetAndIncrement(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicLongAddAndGet(gcj::JniContext* ctx, void* obj, long long value) {
        return ctx->AtomicLongAddAndGet(static_cast<jobject>(obj), value);
    }

    long long IGNITE_CALL IgniteAtomicLongGetAndAdd(gcj::JniContext* ctx, void* obj, long long value) {
        return ctx->AtomicLongGetAndAdd(static_cast<jobject>(obj), value);
    }

    long long IGNITE_CALL IgniteAtomicLongDecrementAndGet(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongDecrementAndGet(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicLongGetAndDecrement(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongGetAndDecrement(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicLongGetAndSet(gcj::JniContext* ctx, void* obj, long long value) {
        return ctx->AtomicLongGetAndSet(static_cast<jobject>(obj), value);
    }

    long long IGNITE_CALL IgniteAtomicLongCompareAndSetAndGet(gcj::JniContext* ctx, void* obj, long long expVal, long long newVal) {
        return ctx->AtomicLongCompareAndSetAndGet(static_cast<jobject>(obj), expVal, newVal);
    }

    bool IGNITE_CALL IgniteAtomicLongIsClosed(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongIsClosed(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteAtomicLongClose(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicLongClose(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicSequenceGet(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicSequenceGet(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicSequenceIncrementAndGet(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicSequenceIncrementAndGet(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicSequenceGetAndIncrement(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicSequenceGetAndIncrement(static_cast<jobject>(obj));
    }

    long long IGNITE_CALL IgniteAtomicSequenceAddAndGet(gcj::JniContext* ctx, void* obj, long long l) {
        return ctx->AtomicSequenceAddAndGet(static_cast<jobject>(obj), l);
    }

    long long IGNITE_CALL IgniteAtomicSequenceGetAndAdd(gcj::JniContext* ctx, void* obj, long long l) {
        return ctx->AtomicSequenceGetAndAdd(static_cast<jobject>(obj), l);
    }

    int IGNITE_CALL IgniteAtomicSequenceGetBatchSize(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicSequenceGetBatchSize(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteAtomicSequenceSetBatchSize(gcj::JniContext* ctx, void* obj, int size) {
        return ctx->AtomicSequenceSetBatchSize(static_cast<jobject>(obj), size);
    }

    bool IGNITE_CALL IgniteAtomicSequenceIsClosed(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicSequenceIsClosed(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteAtomicSequenceClose(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicSequenceClose(static_cast<jobject>(obj));
    }

    bool IGNITE_CALL IgniteAtomicReferenceIsClosed(gcj::JniContext* ctx, void* obj) {
        return ctx->AtomicReferenceIsClosed(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteAtomicReferenceClose(gcj::JniContext* ctx, void* obj) {
        ctx->AtomicReferenceClose(static_cast<jobject>(obj));
    }
    
    bool IGNITE_CALL IgniteListenableCancel(gcj::JniContext* ctx, void* obj) {
        return ctx->ListenableCancel(static_cast<jobject>(obj));
    }

    bool IGNITE_CALL IgniteListenableIsCancelled(gcj::JniContext* ctx, void* obj) {
        return ctx->ListenableIsCancelled(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteSetConsoleHandler(gcj::ConsoleWriteHandler consoleHandler) {
        gcj::JniContext::SetConsoleHandler(consoleHandler);
    }

    void IGNITE_CALL IgniteRemoveConsoleHandler(gcj::ConsoleWriteHandler consoleHandler) {
        gcj::JniContext::RemoveConsoleHandler(consoleHandler);
    }
}