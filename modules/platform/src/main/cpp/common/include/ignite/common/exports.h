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

#ifndef _IGNITE_COMMON_EXPORTS
#define _IGNITE_COMMON_EXPORTS

#include "ignite/common/java.h"

namespace gcj = ignite::common::java;

extern "C" {
    int IGNITE_CALL IgniteReallocate(long long memPtr, int cap);

    void* IGNITE_CALL IgniteIgnitionStart(gcj::JniContext* ctx, char* cfgPath, char* name, int factoryId, long long dataPtr);
    void* IGNITE_CALL IgniteIgnitionInstance(gcj::JniContext* ctx, char* name);
    long long IGNITE_CALL IgniteIgnitionEnvironmentPointer(gcj::JniContext* ctx, char* name);
    bool IGNITE_CALL IgniteIgnitionStop(gcj::JniContext* ctx, char* name, bool cancel);
    void IGNITE_CALL IgniteIgnitionStopAll(gcj::JniContext* ctx, bool cancel);

    void IGNITE_CALL IgniteProcessorReleaseStart(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProcessorProjection(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProcessorCache(gcj::JniContext* ctx, void* obj, char* name);
    void* IGNITE_CALL IgniteProcessorCreateCache(gcj::JniContext* ctx, void* obj, char* name);
    void* IGNITE_CALL IgniteProcessorGetOrCreateCache(gcj::JniContext* ctx, void* obj, char* name);
    void* IGNITE_CALL IgniteProcessorAffinity(gcj::JniContext* ctx, void* obj, char* name);
    void* IGNITE_CALL IgniteProcessorDataStreamer(gcj::JniContext* ctx, void* obj, char* name, bool keepPortable);
    void* IGNITE_CALL IgniteProcessorTransactions(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProcessorCompute(gcj::JniContext* ctx, void* obj, void* prj);
    void* IGNITE_CALL IgniteProcessorMessage(gcj::JniContext* ctx, void* obj, void* prj);
    void* IGNITE_CALL IgniteProcessorEvents(gcj::JniContext* ctx, void* obj, void* prj);
    void* IGNITE_CALL IgniteProcessorServices(gcj::JniContext* ctx, void* obj, void* prj);
    void* IGNITE_CALL IgniteProcessorExtensions(gcj::JniContext* ctx, void* obj);
    
    long long IGNITE_CALL IgniteTargetInStreamOutLong(gcj::JniContext* ctx, void* obj, int opType, long long memPtr);
    void IGNITE_CALL IgniteTargetInStreamOutStream(gcj::JniContext* ctx, void* obj, int opType, long long inMemPtr, long long outMemPtr);
    void* IGNITE_CALL IgniteTargetInStreamOutObject(gcj::JniContext* ctx, void* obj, int opType, long long memPtr);
    void IGNITE_CALL IgniteTargetInObjectStreamOutStream(gcj::JniContext* ctx, void* obj, int opType, void* arg, long long inMemPtr, long long outMemPtr);
    long long IGNITE_CALL IgniteTargetOutLong(gcj::JniContext* ctx, void* obj, int opType);
    void IGNITE_CALL IgniteTargetOutStream(gcj::JniContext* ctx, void* obj, int opType, long long memPtr);
    void* IGNITE_CALL IgniteTargetOutObject(gcj::JniContext* ctx, void* obj, int opType);
    void IGNITE_CALL IgniteTargetListenFuture(gcj::JniContext* ctx, void* obj, long long futId, int typ);
    void IGNITE_CALL IgniteTargetListenFutureForOperation(gcj::JniContext* ctx, void* obj, long long futId, int typ, int opId);

    int IGNITE_CALL IgniteAffinityPartitions(gcj::JniContext* ctx, void* obj);

    void* IGNITE_CALL IgniteCacheWithSkipStore(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteCacheWithNoRetries(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteCacheWithExpiryPolicy(gcj::JniContext* ctx, void* obj, long long create, long long update, long long access);
    void* IGNITE_CALL IgniteCacheWithAsync(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteCacheWithKeepPortable(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteCacheClear(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteCacheRemoveAll(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteCacheOutOpQueryCursor(gcj::JniContext* ctx, void* obj, int type, long long memPtr);
    void* IGNITE_CALL IgniteCacheOutOpContinuousQuery(gcj::JniContext* ctx, void* obj, int type, long long memPtr);
    void* IGNITE_CALL IgniteCacheIterator(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteCacheLocalIterator(gcj::JniContext* ctx, void* obj, int peekModes);
    void IGNITE_CALL IgniteCacheEnterLock(gcj::JniContext* ctx, void* obj, long long id);
    void IGNITE_CALL IgniteCacheExitLock(gcj::JniContext* ctx, void* obj, long long id);
    bool IGNITE_CALL IgniteCacheTryEnterLock(gcj::JniContext* ctx, void* obj, long long id, long long timeout);
    void IGNITE_CALL IgniteCacheCloseLock(gcj::JniContext* ctx, void* obj, long long id);
    void IGNITE_CALL IgniteCacheRebalance(gcj::JniContext* ctx, void* obj, long long futId);
    int IGNITE_CALL IgniteCacheSize(gcj::JniContext* ctx, void* obj, int peekModes, bool loc);

    void IGNITE_CALL IgniteCacheStoreCallbackInvoke(gcj::JniContext* ctx, void* obj, long long memPtr);

    void IGNITE_CALL IgniteComputeWithNoFailover(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteComputeWithTimeout(gcj::JniContext* ctx, void* obj, long long timeout);
    void IGNITE_CALL IgniteComputeExecuteNative(gcj::JniContext* ctx, void* obj, long long taskPtr, long long topVer);

    void IGNITE_CALL IgniteContinuousQueryClose(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteContinuousQueryGetInitialQueryCursor(gcj::JniContext* ctx, void* obj);

    void IGNITE_CALL IgniteDataStreamerListenTopology(gcj::JniContext* ctx, void* obj, long long ptr);
    bool IGNITE_CALL IgniteDataStreamerAllowOverwriteGet(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteDataStreamerAllowOverwriteSet(gcj::JniContext* ctx, void* obj, bool val);
    bool IGNITE_CALL IgniteDataStreamerSkipStoreGet(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteDataStreamerSkipStoreSet(gcj::JniContext* ctx, void* obj, bool val);
    int IGNITE_CALL IgniteDataStreamerPerNodeBufferSizeGet(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteDataStreamerPerNodeBufferSizeSet(gcj::JniContext* ctx, void* obj, int val);
    int IGNITE_CALL IgniteDataStreamerPerNodeParallelOperationsGet(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteDataStreamerPerNodeParallelOperationsSet(gcj::JniContext* ctx, void* obj, int val);

    void* IGNITE_CALL IgniteMessagingWithAsync(gcj::JniContext* ctx, void* obj);

    void* IGNITE_CALL IgniteProjectionForOthers(gcj::JniContext* ctx, void* obj, void* prj);
    void* IGNITE_CALL IgniteProjectionForRemotes(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProjectionForDaemons(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProjectionForRandom(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProjectionForOldest(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProjectionForYoungest(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteProjectionResetMetrics(gcj::JniContext* ctx, void* obj);
    void* IGNITE_CALL IgniteProjectionOutOpRet(gcj::JniContext* ctx, void* obj, int type, long long memPtr);

    void IGNITE_CALL IgniteQueryCursorIterator(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteQueryCursorClose(gcj::JniContext* ctx, void* obj);

    long long IGNITE_CALL IgniteTransactionsStart(gcj::JniContext* ctx, void* obj, int concurrency, int isolation, long long timeout, int txSize);
    int IGNITE_CALL IgniteTransactionsCommit(gcj::JniContext* ctx, void* obj, long long id);
    void IGNITE_CALL IgniteTransactionsCommitAsync(gcj::JniContext* ctx, void* obj, long long id, long long futId);
    int IGNITE_CALL IgniteTransactionsRollback(gcj::JniContext* ctx, void* obj, long long id);
    void IGNITE_CALL IgniteTransactionsRollbackAsync(gcj::JniContext* ctx, void* obj, long long id, long long futId);
    int IGNITE_CALL IgniteTransactionsClose(gcj::JniContext* ctx, void* obj, long long id);
    int IGNITE_CALL IgniteTransactionsState(gcj::JniContext* ctx, void* obj, long long id);
    bool IGNITE_CALL IgniteTransactionsSetRollbackOnly(gcj::JniContext* ctx, void* obj, long long id);
    void IGNITE_CALL IgniteTransactionsResetMetrics(gcj::JniContext* ctx, void* obj);

    void* IGNITE_CALL IgniteAcquire(gcj::JniContext* ctx, void* obj);
    void IGNITE_CALL IgniteRelease(void* obj);

    void IGNITE_CALL IgniteThrowToJava(gcj::JniContext* ctx, char* errMsg);
    
    int IGNITE_CALL IgniteHandlersSize();

    void* IGNITE_CALL IgniteCreateContext(char** opts, int optsLen, gcj::JniHandlers* cbs);
    void IGNITE_CALL IgniteDeleteContext(gcj::JniContext* ctx);

    void IGNITE_CALL IgniteDestroyJvm(gcj::JniContext* ctx);

    void* IGNITE_CALL IgniteEventsWithAsync(gcj::JniContext* ctx, void* obj);
    bool IGNITE_CALL IgniteEventsStopLocalListen(gcj::JniContext* ctx, void* obj, long long hnd);
    void IGNITE_CALL IgniteEventsLocalListen(gcj::JniContext* ctx, void* obj, long long hnd, int type);
    bool IGNITE_CALL IgniteEventsIsEnabled(gcj::JniContext* ctx, void* obj, int type);
        
	void* IGNITE_CALL IgniteServicesWithAsync(gcj::JniContext* ctx, void* obj);
	void* IGNITE_CALL IgniteServicesWithServerKeepPortable(gcj::JniContext* ctx, void* obj);
	void IGNITE_CALL IgniteServicesCancel(gcj::JniContext* ctx, void* obj, char* name);
	void IGNITE_CALL IgniteServicesCancelAll(gcj::JniContext* ctx, void* obj);
	void* IGNITE_CALL IgniteServicesGetServiceProxy(gcj::JniContext* ctx, void* obj, char* name, bool sticky);
}

#endif