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

#ifndef _IGNITE_COMMON_JVM
#define _IGNITE_COMMON_JVM

#include <jni.h>

#include "ignite/common/common.h"

namespace ignite
{
    namespace common
    {
        namespace java
        {
            /* Error constants. */
            const int IGNITE_JNI_ERR_SUCCESS = 0;
            const int IGNITE_JNI_ERR_GENERIC = 1;
            const int IGNITE_JNI_ERR_JVM_INIT = 2;
            const int IGNITE_JNI_ERR_JVM_ATTACH = 3;

            /* Handlers for callbacks from Java. */
            typedef long long(JNICALL *CacheStoreCreateHandler)(void* target, long long memPtr);
            typedef int(JNICALL *CacheStoreInvokeHandler)(void* target, long long objPtr, long long memPtr, void* cb);
            typedef void(JNICALL *CacheStoreDestroyHandler)(void* target, long long objPtr);
            typedef long long(JNICALL *CacheStoreSessionCreateHandler)(void* target, long long storePtr);

            typedef long long(JNICALL *CacheEntryFilterCreateHandler)(void* target, long long memPtr);
            typedef int(JNICALL *CacheEntryFilterApplyHandler)(void* target, long long ptr, long long memPtr);
            typedef void(JNICALL *CacheEntryFilterDestroyHandler)(void* target, long long ptr);

            typedef void(JNICALL *CacheInvokeHandler)(void* target, long long inMemPtr, long long outMemPtr);

            typedef void(JNICALL *ComputeTaskMapHandler)(void* target, long long taskPtr, long long inMemPtr, long long outMemPtr);
            typedef int(JNICALL *ComputeTaskJobResultHandler)(void* target, long long taskPtr, long long jobPtr, long long memPtr);
            typedef void(JNICALL *ComputeTaskReduceHandler)(void* target, long long taskPtr);
            typedef void(JNICALL *ComputeTaskCompleteHandler)(void* target, long long taskPtr, long long memPtr);
            typedef int(JNICALL *ComputeJobSerializeHandler)(void* target, long long jobPtr, long long memPtr);
            typedef long long(JNICALL *ComputeJobCreateHandler)(void* target, long long memPtr);
            typedef void(JNICALL *ComputeJobExecuteHandler)(void* target, long long jobPtr, int cancel, long long memPtr);
            typedef void(JNICALL *ComputeJobCancelHandler)(void* target, long long jobPtr);
            typedef void(JNICALL *ComputeJobDestroyHandler)(void* target, long long jobPtr);

            typedef void(JNICALL *ContinuousQueryListenerApplyHandler)(void* target, long long lsnrPtr, long long memPtr);
            typedef long long(JNICALL *ContinuousQueryFilterCreateHandler)(void* target, long long memPtr);
            typedef int(JNICALL *ContinuousQueryFilterApplyHandler)(void* target, long long filterPtr, long long memPtr);
            typedef void(JNICALL *ContinuousQueryFilterReleaseHandler)(void* target, long long filterPtr);

			typedef void(JNICALL *DataStreamerTopologyUpdateHandler)(void* target, long long ldrPtr, long long topVer, int topSize);
			typedef void(JNICALL *DataStreamerStreamReceiverInvokeHandler)(void* target, long long ptr, void* cache, long long memPtr, unsigned char keepPortable);

            typedef void(JNICALL *FutureByteResultHandler)(void* target, long long futAddr, int res);
            typedef void(JNICALL *FutureBoolResultHandler)(void* target, long long futAddr, int res);
            typedef void(JNICALL *FutureShortResultHandler)(void* target, long long futAddr, int res);
            typedef void(JNICALL *FutureCharResultHandler)(void* target, long long futAddr, int res);
            typedef void(JNICALL *FutureIntResultHandler)(void* target, long long futAddr, int res);
            typedef void(JNICALL *FutureFloatResultHandler)(void* target, long long futAddr, float res);
            typedef void(JNICALL *FutureLongResultHandler)(void* target, long long futAddr, long long res);
            typedef void(JNICALL *FutureDoubleResultHandler)(void* target, long long futAddr, double res);
            typedef void(JNICALL *FutureObjectResultHandler)(void* target, long long futAddr, long long memPtr);
            typedef void(JNICALL *FutureNullResultHandler)(void* target, long long futAddr);
            typedef void(JNICALL *FutureErrorHandler)(void* target, long long futAddr, long long memPtr);

            typedef void(JNICALL *LifecycleEventHandler)(void* target, long long ptr, int evt);

            typedef void(JNICALL *MemoryReallocateHandler)(void* target, long long memPtr, int cap);

            typedef long long(JNICALL *MessagingFilterCreateHandler)(void* target, long long memPtr);
            typedef int(JNICALL *MessagingFilterApplyHandler)(void* target, long long ptr, long long memPtr);
            typedef void(JNICALL *MessagingFilterDestroyHandler)(void* target, long long ptr);

            typedef long long(JNICALL *EventFilterCreateHandler)(void* target, long long memPtr);
            typedef int(JNICALL *EventFilterApplyHandler)(void* target, long long ptr, long long memPtr);
            typedef void(JNICALL *EventFilterDestroyHandler)(void* target, long long ptr);

			typedef long long(JNICALL *ServiceInitHandler)(void* target, long long memPtr);
			typedef void(JNICALL *ServiceExecuteHandler)(void* target, long long svcPtr, long long memPtr);
			typedef void(JNICALL *ServiceCancelHandler)(void* target, long long svcPtr, long long memPtr);
			typedef void(JNICALL *ServiceInvokeMethodHandler)(void* target, long long svcPtr, long long inMemPtr, long long outMemPtr);
			typedef int(JNICALL *ClusterNodeFilterApplyHandler)(void* target, long long memPtr);

            typedef long long(JNICALL *NodeInfoHandler)(void* target, long long memPtr);

            typedef void(JNICALL *OnStartHandler)(void* target, void* proc, long long memPtr);
            typedef void(JNICALL *OnStopHandler)(void* target);
            typedef void(JNICALL *ErrorHandler)(void* target, int errCode, const char* errClsChars, int errClsCharsLen, const char* errMsgChars, int errMsgCharsLen, void* errData, int errDataLen);

            typedef long long(JNICALL *ExtensionCallbackInLongOutLongHandler)(void* target, int typ, long long arg1);
            typedef long long(JNICALL *ExtensionCallbackInLongLongOutLongHandler)(void* target, int typ, long long arg1, long long arg2);

            /**
             * JNI handlers holder.
             */
            struct JniHandlers {
                void* target;

                CacheStoreCreateHandler cacheStoreCreate;
                CacheStoreInvokeHandler cacheStoreInvoke;
                CacheStoreDestroyHandler cacheStoreDestroy;
                CacheStoreSessionCreateHandler cacheStoreSessionCreate;

                CacheEntryFilterCreateHandler cacheEntryFilterCreate;
                CacheEntryFilterApplyHandler cacheEntryFilterApply;
                CacheEntryFilterDestroyHandler cacheEntryFilterDestroy;

                CacheInvokeHandler cacheInvoke;

                ComputeTaskMapHandler computeTaskMap;
                ComputeTaskJobResultHandler computeTaskJobRes;
                ComputeTaskReduceHandler computeTaskReduce;
                ComputeTaskCompleteHandler computeTaskComplete;
                ComputeJobSerializeHandler computeJobSerialize;
                ComputeJobCreateHandler computeJobCreate;
                ComputeJobExecuteHandler computeJobExec;
                ComputeJobCancelHandler computeJobCancel;
                ComputeJobDestroyHandler computeJobDestroy;

                ContinuousQueryListenerApplyHandler contQryLsnrApply;
                ContinuousQueryFilterCreateHandler contQryFilterCreate;
                ContinuousQueryFilterApplyHandler contQryFilterApply;
                ContinuousQueryFilterReleaseHandler contQryFilterRelease;

				DataStreamerTopologyUpdateHandler dataStreamerTopologyUpdate;
				DataStreamerStreamReceiverInvokeHandler streamReceiverInvoke;

                FutureByteResultHandler futByteRes;
                FutureBoolResultHandler futBoolRes;
                FutureShortResultHandler futShortRes;
                FutureCharResultHandler futCharRes;
                FutureIntResultHandler futIntRes;
                FutureFloatResultHandler futFloatRes;
                FutureLongResultHandler futLongRes;
                FutureDoubleResultHandler futDoubleRes;
                FutureObjectResultHandler futObjRes;
                FutureNullResultHandler futNullRes;
                FutureErrorHandler futErr;

                LifecycleEventHandler lifecycleEvt;

                MemoryReallocateHandler memRealloc;

                MessagingFilterCreateHandler messagingFilterCreate;
                MessagingFilterApplyHandler messagingFilterApply;
                MessagingFilterDestroyHandler messagingFilterDestroy;
                
                EventFilterCreateHandler eventFilterCreate;
                EventFilterApplyHandler eventFilterApply;
                EventFilterDestroyHandler eventFilterDestroy;

				ServiceInitHandler serviceInit;
				ServiceExecuteHandler serviceExecute;
				ServiceCancelHandler serviceCancel;
				ServiceInvokeMethodHandler serviceInvokeMethod;
				
				ClusterNodeFilterApplyHandler clusterNodeFilterApply;

                NodeInfoHandler nodeInfo;

                OnStartHandler onStart;
                OnStopHandler onStop;
                ErrorHandler error;

                ExtensionCallbackInLongOutLongHandler extensionCallbackInLongOutLong;
                ExtensionCallbackInLongLongOutLongHandler extensionCallbackInLongLongOutLong;
            };

            /**
             * JNI Java members.
             */
            struct JniJavaMembers {
                jclass c_Class;
                jmethodID m_Class_getName;

                jclass c_Throwable;
                jmethodID m_Throwable_getMessage;
                jmethodID m_Throwable_printStackTrace;

                /**
                 * Constructor.
                 */
                void Initialize(JNIEnv* env);

                /**
                 * Destroy members releasing all allocated classes.
                 */
                void Destroy(JNIEnv* env);

                /**
                 * Write error information.
                 */
                bool WriteErrorInfo(JNIEnv* env, char** errClsName, int* errClsNameLen, char** errMsg, int* errMsgLen);
            };

            /**
             * JNI members.
             */
            struct JniMembers {
                jclass c_PlatformAbstractQryCursor;
                jmethodID m_PlatformAbstractQryCursor_iter;
                jmethodID m_PlatformAbstractQryCursor_iterHasNext;
                jmethodID m_PlatformAbstractQryCursor_close;

                jclass c_PlatformAffinity;
                jmethodID m_PlatformAffinity_partitions;

                jclass c_PlatformCache;
                jmethodID m_PlatformCache_withSkipStore;
                jmethodID m_PlatformCache_withNoRetries;
                jmethodID m_PlatformCache_withExpiryPolicy;
                jmethodID m_PlatformCache_withAsync;
                jmethodID m_PlatformCache_withKeepPortable;
                jmethodID m_PlatformCache_clear;
                jmethodID m_PlatformCache_removeAll;
                jmethodID m_PlatformCache_iterator;
                jmethodID m_PlatformCache_localIterator;
                jmethodID m_PlatformCache_enterLock;
                jmethodID m_PlatformCache_exitLock;
                jmethodID m_PlatformCache_tryEnterLock;
                jmethodID m_PlatformCache_closeLock;
                jmethodID m_PlatformCache_rebalance;
                jmethodID m_PlatformCache_size;

                jclass c_PlatformCacheStoreCallback;
                jmethodID m_PlatformCacheStoreCallback_invoke;

                jclass c_IgniteException;

                jclass c_PlatformClusterGroup;
                jmethodID m_PlatformClusterGroup_forOthers;
                jmethodID m_PlatformClusterGroup_forRemotes;
                jmethodID m_PlatformClusterGroup_forDaemons;
                jmethodID m_PlatformClusterGroup_forRandom;
                jmethodID m_PlatformClusterGroup_forOldest;
                jmethodID m_PlatformClusterGroup_forYoungest;
                jmethodID m_PlatformClusterGroup_resetMetrics;

                jclass c_PlatformCompute;
                jmethodID m_PlatformCompute_withNoFailover;
                jmethodID m_PlatformCompute_withTimeout;
                jmethodID m_PlatformCompute_executeNative;

                jclass c_PlatformContinuousQuery;
                jmethodID m_PlatformContinuousQuery_close;
                jmethodID m_PlatformContinuousQuery_getInitialQueryCursor;

                jclass c_PlatformDataStreamer;
                jmethodID m_PlatformDataStreamer_listenTopology;
                jmethodID m_PlatformDataStreamer_getAllowOverwrite;
                jmethodID m_PlatformDataStreamer_setAllowOverwrite;
                jmethodID m_PlatformDataStreamer_getSkipStore;
                jmethodID m_PlatformDataStreamer_setSkipStore;
                jmethodID m_PlatformDataStreamer_getPerNodeBufSize;
                jmethodID m_PlatformDataStreamer_setPerNodeBufSize;
                jmethodID m_PlatformDataStreamer_getPerNodeParallelOps;
                jmethodID m_PlatformDataStreamer_setPerNodeParallelOps;
                
                jclass c_PlatformEvents;
                jmethodID m_PlatformEvents_withAsync;
                jmethodID m_PlatformEvents_stopLocalListen;
                jmethodID m_PlatformEvents_localListen;
                jmethodID m_PlatformEvents_isEnabled;
                
				jclass c_PlatformServices;
				jmethodID m_PlatformServices_withAsync;
				jmethodID m_PlatformServices_withServerKeepPortable;
				jmethodID m_PlatformServices_cancel;
				jmethodID m_PlatformServices_cancelAll;
				jmethodID m_PlatformServices_serviceProxy;

				jclass c_PlatformIgnition;
                jmethodID m_PlatformIgnition_start;
                jmethodID m_PlatformIgnition_instance;
                jmethodID m_PlatformIgnition_environmentPointer;
                jmethodID m_PlatformIgnition_stop;
                jmethodID m_PlatformIgnition_stopAll;

                jclass c_PlatformMessaging;
                jmethodID m_PlatformMessaging_withAsync;

                jclass c_PlatformProcessor;
                jmethodID m_PlatformProcessor_releaseStart;
                jmethodID m_PlatformProcessor_cache;
                jmethodID m_PlatformProcessor_createCache;
                jmethodID m_PlatformProcessor_getOrCreateCache;
                jmethodID m_PlatformProcessor_affinity;
                jmethodID m_PlatformProcessor_dataStreamer;
                jmethodID m_PlatformProcessor_transactions;
                jmethodID m_PlatformProcessor_projection;
                jmethodID m_PlatformProcessor_compute;
                jmethodID m_PlatformProcessor_message;
                jmethodID m_PlatformProcessor_events;
                jmethodID m_PlatformProcessor_services;
                jmethodID m_PlatformProcessor_extensions;

                jclass c_PlatformTarget;
                jmethodID m_PlatformTarget_inStreamOutLong;
                jmethodID m_PlatformTarget_inStreamOutObject;
                jmethodID m_PlatformTarget_outLong;
                jmethodID m_PlatformTarget_outStream;
                jmethodID m_PlatformTarget_outObject;
                jmethodID m_PlatformTarget_inStreamOutStream;
                jmethodID m_PlatformTarget_inObjectStreamOutStream;
                jmethodID m_PlatformTarget_listenFuture;
                jmethodID m_PlatformTarget_listenFutureForOperation;

                jclass c_PlatformTransactions;
                jmethodID m_PlatformTransactions_txStart;
                jmethodID m_PlatformTransactions_txCommit;
                jmethodID m_PlatformTransactions_txCommitAsync;
                jmethodID m_PlatformTransactions_txRollback;
                jmethodID m_PlatformTransactions_txRollbackAsync;
                jmethodID m_PlatformTransactions_txState;
                jmethodID m_PlatformTransactions_txSetRollbackOnly;
                jmethodID m_PlatformTransactions_txClose;
                jmethodID m_PlatformTransactions_resetMetrics;

                jclass c_PlatformUtils;
                jmethodID m_PlatformUtils_reallocate;
                jmethodID m_PlatformUtils_errData;

                /**
                 * Constructor.
                 */
                void Initialize(JNIEnv* env);

                /**
                 * Destroy members releasing all allocated classes.
                 */
                void Destroy(JNIEnv* env);
            };

            /**
             * JNI JVM wrapper.
             */
            class IGNITE_IMPORT_EXPORT JniJvm {
            public:
                /**
                 * Default constructor for uninitialized JVM.
                 */
                JniJvm();

                /**
                 * Constructor.
                 *
                 * @param jvm JVM.
                 * @param javaMembers Java members.
                 * @param members Members.
                 */
                JniJvm(JavaVM* jvm, JniJavaMembers javaMembers, JniMembers members);

                /**
                 * Get JVM.
                 *
                 * @param JVM.
                 */
                JavaVM* GetJvm();

                /**
                 * Get Java members.
                 *
                 * @param Java members.
                 */
                JniJavaMembers& GetJavaMembers();

                /**
                 * Get members.
                 *
                 * @param Members.
                 */
                JniMembers& GetMembers();
            private:
                /** JVM. */
                JavaVM* jvm;

                /** Java members. */
                JniJavaMembers javaMembers;

                /** Members. */
                JniMembers members;
            };

            /**
             * JNI error information.
             */
            struct IGNITE_IMPORT_EXPORT JniErrorInfo
            {
                int code;
                char* errCls;
                char* errMsg;

                /**
                 * Default constructor. Creates empty error info.
                 */
                JniErrorInfo();

                /**
                 * Constructor.
                 *
                 * @param code Code.
                 * @param errCls Error class.
                 * @param errMsg Error message.
                 */
                JniErrorInfo(int code, const char* errCls, const char* errMsg);

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                JniErrorInfo(const JniErrorInfo& other);

                /**
                 * Assignment operator overload.
                 *
                 * @param other Other instance.
                 * @return This instance.
                 */
                JniErrorInfo& operator=(const JniErrorInfo& other);

                /**
                 * Destructor.
                 */
                ~JniErrorInfo();
            };

            /**
             * Unmanaged context.
             */
            class IGNITE_IMPORT_EXPORT JniContext {
            public:
                static JniContext* Create(char** opts, int optsLen, JniHandlers hnds);
                static JniContext* Create(char** opts, int optsLen, JniHandlers hnds, JniErrorInfo* errInfo);
                static int Reallocate(long long memPtr, int cap);
                static void Detach();
                static void Release(jobject obj);

                jobject IgnitionStart(char* cfgPath, char* name, int factoryId, long long dataPtr);
                jobject IgnitionStart(char* cfgPath, char* name, int factoryId, long long dataPtr, JniErrorInfo* errInfo);
                jobject IgnitionInstance(char* name);
                jobject IgnitionInstance(char* name, JniErrorInfo* errInfo);
                long long IgnitionEnvironmentPointer(char* name);
                long long IgnitionEnvironmentPointer(char* name, JniErrorInfo* errInfo);
                bool IgnitionStop(char* name, bool cancel);
                bool IgnitionStop(char* name, bool cancel, JniErrorInfo* errInfo);
                void IgnitionStopAll(bool cancel);
                void IgnitionStopAll(bool cancel, JniErrorInfo* errInfo);
                
                void ProcessorReleaseStart(jobject obj);
                jobject ProcessorProjection(jobject obj);
                jobject ProcessorCache(jobject obj, const char* name);
                jobject ProcessorCache(jobject obj, const char* name, JniErrorInfo* errInfo);
                jobject ProcessorCreateCache(jobject obj, const char* name);
                jobject ProcessorCreateCache(jobject obj, const char* name, JniErrorInfo* errInfo);
                jobject ProcessorGetOrCreateCache(jobject obj, const char* name);
                jobject ProcessorGetOrCreateCache(jobject obj, const char* name, JniErrorInfo* errInfo);
                jobject ProcessorAffinity(jobject obj, const char* name);
                jobject ProcessorDataStreamer(jobject obj, const char* name, bool keepPortable);
                jobject ProcessorTransactions(jobject obj);
                jobject ProcessorCompute(jobject obj, jobject prj);
                jobject ProcessorMessage(jobject obj, jobject prj);
                jobject ProcessorEvents(jobject obj, jobject prj);
                jobject ProcessorServices(jobject obj, jobject prj);
                jobject ProcessorExtensions(jobject obj);
                
                long long TargetInStreamOutLong(jobject obj, int type, long long memPtr, JniErrorInfo* errInfo = NULL);
                void TargetInStreamOutStream(jobject obj, int opType, long long inMemPtr, long long outMemPtr, JniErrorInfo* errInfo = NULL);
                jobject TargetInStreamOutObject(jobject obj, int type, long long memPtr, JniErrorInfo* errInfo = NULL);
                void TargetInObjectStreamOutStream(jobject obj, int opType, void* arg, long long inMemPtr, long long outMemPtr, JniErrorInfo* errInfo = NULL);
                long long TargetOutLong(jobject obj, int opType, JniErrorInfo* errInfo = NULL);
                void TargetOutStream(jobject obj, int opType, long long memPtr, JniErrorInfo* errInfo = NULL);
                jobject TargetOutObject(jobject obj, int opType, JniErrorInfo* errInfo = NULL);
                void TargetListenFuture(jobject obj, long long futId, int typ);
                void TargetListenFutureForOperation(jobject obj, long long futId, int typ, int opId);
                
                int AffinityPartitions(jobject obj);

                jobject CacheWithSkipStore(jobject obj);
                jobject CacheWithNoRetries(jobject obj);
                jobject CacheWithExpiryPolicy(jobject obj, long long create, long long update, long long access);
                jobject CacheWithAsync(jobject obj);
                jobject CacheWithKeepPortable(jobject obj);
                void CacheClear(jobject obj, JniErrorInfo* errInfo = NULL);
                void CacheRemoveAll(jobject obj, JniErrorInfo* errInfo = NULL);
                jobject CacheOutOpQueryCursor(jobject obj, int type, long long memPtr, JniErrorInfo* errInfo = NULL);
                jobject CacheOutOpContinuousQuery(jobject obj, int type, long long memPtr);
                jobject CacheIterator(jobject obj);
                jobject CacheLocalIterator(jobject obj, int peekModes);
                void CacheEnterLock(jobject obj, long long id);
                void CacheExitLock(jobject obj, long long id);
                bool CacheTryEnterLock(jobject obj, long long id, long long timeout);
                void CacheCloseLock(jobject obj, long long id);
                void CacheRebalance(jobject obj, long long futId);
                int CacheSize(jobject obj, int peekModes, bool loc, JniErrorInfo* errInfo = NULL);

                void CacheStoreCallbackInvoke(jobject obj, long long memPtr);

                void ComputeWithNoFailover(jobject obj);
                void ComputeWithTimeout(jobject obj, long long timeout);
                void ComputeExecuteNative(jobject obj, long long taskPtr, long long topVer);

                void ContinuousQueryClose(jobject obj);
                void* ContinuousQueryGetInitialQueryCursor(jobject obj);

                void DataStreamerListenTopology(jobject obj, long long ptr);
                bool DataStreamerAllowOverwriteGet(jobject obj);
                void DataStreamerAllowOverwriteSet(jobject obj, bool val);
                bool DataStreamerSkipStoreGet(jobject obj);
                void DataStreamerSkipStoreSet(jobject obj, bool val);
                int DataStreamerPerNodeBufferSizeGet(jobject obj);
                void DataStreamerPerNodeBufferSizeSet(jobject obj, int val);
                int DataStreamerPerNodeParallelOperationsGet(jobject obj);
                void DataStreamerPerNodeParallelOperationsSet(jobject obj, int val);

                jobject MessagingWithAsync(jobject obj);

                jobject ProjectionForOthers(jobject obj, jobject prj);
                jobject ProjectionForRemotes(jobject obj);
                jobject ProjectionForDaemons(jobject obj);
                jobject ProjectionForRandom(jobject obj);
                jobject ProjectionForOldest(jobject obj);
                jobject ProjectionForYoungest(jobject obj);
                void ProjectionResetMetrics(jobject obj);
                jobject ProjectionOutOpRet(jobject obj, int type, long long memPtr);

                void QueryCursorIterator(jobject obj, JniErrorInfo* errInfo = NULL);
                bool QueryCursorIteratorHasNext(jobject obj, JniErrorInfo* errInfo = NULL);
                void QueryCursorClose(jobject obj, JniErrorInfo* errInfo = NULL);

                long long TransactionsStart(jobject obj, int concurrency, int isolation, long long timeout, int txSize);
                int TransactionsCommit(jobject obj, long long id);
                void TransactionsCommitAsync(jobject obj, long long id, long long futId);
                int TransactionsRollback(jobject obj, long long id);
                void TransactionsRollbackAsync(jobject obj, long long id, long long futId);
                int TransactionsClose(jobject obj, long long id);
                int TransactionsState(jobject obj, long long id);
                bool TransactionsSetRollbackOnly(jobject obj, long long id);
                void TransactionsResetMetrics(jobject obj);

                jobject EventsWithAsync(jobject obj);
                bool EventsStopLocalListen(jobject obj, long long hnd);
                void EventsLocalListen(jobject obj, long long hnd, int type);
                bool EventsIsEnabled(jobject obj, int type);
                
				jobject ServicesWithAsync(jobject obj);
                jobject ServicesWithServerKeepPortable(jobject obj);
				void ServicesCancel(jobject obj, char* name);
				void ServicesCancelAll(jobject obj);
				void* ServicesGetServiceProxy(jobject obj, char* name, bool sticky);

                jobject Acquire(jobject obj);

                void DestroyJvm();
                void ThrowToJava(char* errMsg);
            private:
                JniJvm* jvm;
                JniHandlers hnds;

                JniContext(JniJvm* jvm, JniHandlers hnds);

                JNIEnv* Attach();
                void ExceptionCheck(JNIEnv* env);
                void ExceptionCheck(JNIEnv* env, JniErrorInfo* errInfo);
                jobject LocalToGlobal(JNIEnv* env, jobject obj);
                jobject ProcessorCache0(jobject proc, const char* name, jmethodID mthd, JniErrorInfo* errInfo);
            };

            JNIEXPORT jlong JNICALL JniCacheStoreCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniCacheStoreInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr, jlong memPtr, jobject cb);
            JNIEXPORT void JNICALL JniCacheStoreDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr);
            JNIEXPORT jlong JNICALL JniCacheStoreSessionCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong storePtr);

            JNIEXPORT jlong JNICALL JniCacheEntryFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniCacheEntryFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr, jlong memPtr);
            JNIEXPORT void JNICALL JniCacheEntryFilterDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr);

            JNIEXPORT void JNICALL JniCacheInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong inMemPtr, jlong outMemPtr);

            JNIEXPORT void JNICALL JniComputeTaskMap(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr, jlong inMemPtr, jlong outMemPtr);
            JNIEXPORT jint JNICALL JniComputeTaskJobResult(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr, jlong jobPtr, jlong memPtr);
            JNIEXPORT void JNICALL JniComputeTaskReduce(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr);
            JNIEXPORT void JNICALL JniComputeTaskComplete(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniComputeJobSerialize(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr, jlong memPtr);
            JNIEXPORT jlong JNICALL JniComputeJobCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT void JNICALL JniComputeJobExecute(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr, jint cancel, jlong memPtr);
            JNIEXPORT void JNICALL JniComputeJobCancel(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr);
            JNIEXPORT void JNICALL JniComputeJobDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr);

            JNIEXPORT void JNICALL JniContinuousQueryListenerApply(JNIEnv *env, jclass cls, jlong envPtr, jlong cbPtr, jlong memPtr);
            JNIEXPORT jlong JNICALL JniContinuousQueryFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniContinuousQueryFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong filterPtr, jlong memPtr);
            JNIEXPORT void JNICALL JniContinuousQueryFilterRelease(JNIEnv *env, jclass cls, jlong envPtr, jlong filterPtr);

			JNIEXPORT void JNICALL JniDataStreamerTopologyUpdate(JNIEnv *env, jclass cls, jlong envPtr, jlong ldrPtr, jlong topVer, jint topSize);
			JNIEXPORT void JNICALL JniDataStreamerStreamReceiverInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jobject cache, jlong memPtr, jboolean keepPortable);

            JNIEXPORT void JNICALL JniFutureByteResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res);
            JNIEXPORT void JNICALL JniFutureBoolResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res);
            JNIEXPORT void JNICALL JniFutureShortResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res);
            JNIEXPORT void JNICALL JniFutureCharResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res);
            JNIEXPORT void JNICALL JniFutureIntResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res);
            JNIEXPORT void JNICALL JniFutureFloatResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jfloat res);
            JNIEXPORT void JNICALL JniFutureLongResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jlong res);
            JNIEXPORT void JNICALL JniFutureDoubleResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jdouble res);
            JNIEXPORT void JNICALL JniFutureObjectResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jlong memPtr);
            JNIEXPORT void JNICALL JniFutureNullResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr);
            JNIEXPORT void JNICALL JniFutureError(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jlong memPtr);

            JNIEXPORT void JNICALL JniLifecycleEvent(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jint evt);

            JNIEXPORT void JNICALL JniMemoryReallocate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr, jint cap);

            JNIEXPORT jlong JNICALL JniMessagingFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniMessagingFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong memPtr);
            JNIEXPORT void JNICALL JniMessagingFilterDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr);
            
            JNIEXPORT jlong JNICALL JniEventFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniEventFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong memPtr);
            JNIEXPORT void JNICALL JniEventFilterDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr);

			JNIEXPORT jlong JNICALL JniServiceInit(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
			JNIEXPORT void JNICALL JniServiceExecute(JNIEnv *env, jclass cls, jlong envPtr, jlong svcPtr, jlong memPtr);
			JNIEXPORT void JNICALL JniServiceCancel(JNIEnv *env, jclass cls, jlong envPtr, jlong svcPtr, jlong memPtr);
			JNIEXPORT void JNICALL JniServiceInvokeMethod(JNIEnv *env, jclass cls, jlong envPtr, jlong svcPtr, jlong inMemPtr, jlong outMemPtr);
			JNIEXPORT jint JNICALL JniClusterNodeFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);

            JNIEXPORT jlong JNICALL JniNodeInfo(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);

            JNIEXPORT void JNICALL JniOnStart(JNIEnv *env, jclass cls, jlong envPtr, jobject proc, jlong memPtr);
            JNIEXPORT void JNICALL JniOnStop(JNIEnv *env, jclass cls, jlong envPtr);

            JNIEXPORT jlong JNICALL JniExtensionCallbackInLongOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint typ, jlong arg1);
            JNIEXPORT jlong JNICALL JniExtensionCallbackInLongLongOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint typ, jlong arg1, jlong arg2);
        }
    }
}

#endif