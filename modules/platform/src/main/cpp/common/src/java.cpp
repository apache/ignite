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

#include <cstring>
#include <string>
#include <exception>

#include "ignite/common/concurrent.h"
#include "ignite/common/java.h"

#define IGNITE_SAFE_PROC_NO_ARG(jniEnv, envPtr, type, field) { \
    JniHandlers* hnds = reinterpret_cast<JniHandlers*>(envPtr); \
    type hnd = hnds->field; \
    if (hnd) \
        hnd(hnds->target); \
    else \
        ThrowOnMissingHandler(jniEnv); \
}

#define IGNITE_SAFE_PROC(jniEnv, envPtr, type, field, ...) { \
    JniHandlers* hnds = reinterpret_cast<JniHandlers*>(envPtr); \
    type hnd = hnds->field; \
    if (hnd) \
        hnd(hnds->target, __VA_ARGS__); \
    else \
        ThrowOnMissingHandler(jniEnv); \
}

#define IGNITE_SAFE_FUNC(jniEnv, envPtr, type, field, ...) { \
    JniHandlers* hnds = reinterpret_cast<JniHandlers*>(envPtr); \
    type hnd = hnds->field; \
    if (hnd) \
        return hnd(hnds->target, __VA_ARGS__); \
    else \
    { \
        ThrowOnMissingHandler(jniEnv); \
        return 0; \
    }\
}

namespace ignite
{
    namespace common
    {
        namespace java
        {
            namespace gcc = ignite::common::concurrent;

            /* --- Startup exception. --- */
            class JvmException : public std::exception {
                // No-op.
            };

            /* --- JNI method definitions. --- */
            struct JniMethod {
                char* name;
                char* sign;
                bool isStatic;

                JniMethod(const char* name, const char* sign, bool isStatic) {
                    this->name = const_cast<char*>(name);
                    this->sign = const_cast<char*>(sign);
                    this->isStatic = isStatic;
                }
            };

            /*
             * Heloper function to copy characters.
             *
             * @param src Source.
             * @return Result.
             */
            char* CopyChars(const char* src)
            {
                if (src)
                {
                    size_t len = strlen(src);
                    char* dest = new char[len + 1];
                    strcpy(dest, src);
                    *(dest + len) = 0;
                    return dest;
                }
                else
                    return NULL;
            }

            JniErrorInfo::JniErrorInfo() : code(IGNITE_JNI_ERR_SUCCESS), errCls(NULL), errMsg(NULL)
            {
                // No-op.
            }

            JniErrorInfo::JniErrorInfo(int code, const char* errCls, const char* errMsg) : code(code)
            {
                this->errCls = CopyChars(errCls);
                this->errMsg = CopyChars(errMsg);
            }

            JniErrorInfo::JniErrorInfo(const JniErrorInfo& other) : code(other.code)
            {
                this->errCls = CopyChars(other.errCls);
                this->errMsg = CopyChars(other.errMsg);
            }

            JniErrorInfo& JniErrorInfo::operator=(const JniErrorInfo& other)
            {
                if (this != &other)
                {
                    // 1. Create new instance, exception could occur at this point.
                    JniErrorInfo tmp(other);

                    // 2. Swap with temp.
                    int code0 = code;
                    char* errCls0 = errCls;
                    char* errMsg0 = errMsg;

                    code = tmp.code;
                    errCls = tmp.errCls;
                    errMsg = tmp.errMsg;

                    tmp.code = code0;
                    tmp.errCls = errCls0;
                    tmp.errMsg = errMsg0;
                }

                return *this;
            }

            JniErrorInfo::~JniErrorInfo()
            {
                if (errCls)
                    delete[] errCls;

                if (errMsg)
                    delete[] errMsg;
            }

            const char* C_THROWABLE = "java/lang/Throwable";
            JniMethod M_THROWABLE_GET_MESSAGE = JniMethod("getMessage", "()Ljava/lang/String;", false);
            JniMethod M_THROWABLE_PRINT_STACK_TRACE = JniMethod("printStackTrace", "()V", false);

            const char* C_CLASS = "java/lang/Class";
            JniMethod M_CLASS_GET_NAME = JniMethod("getName", "()Ljava/lang/String;", false);

            const char* C_IGNITE_EXCEPTION = "org/apache/ignite/IgniteException";

            const char* C_PLATFORM_NO_CALLBACK_EXCEPTION = "org/apache/ignite/internal/processors/platform/PlatformNoCallbackException";

            const char* C_PLATFORM_PROCESSOR = "org/apache/ignite/internal/processors/platform/PlatformProcessor";
            JniMethod M_PLATFORM_PROCESSOR_RELEASE_START = JniMethod("releaseStart", "()V", false);
            JniMethod M_PLATFORM_PROCESSOR_PROJECTION = JniMethod("projection", "()Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_CACHE = JniMethod("cache", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_CREATE_CACHE = JniMethod("createCache", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_GET_OR_CREATE_CACHE = JniMethod("getOrCreateCache", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_AFFINITY = JniMethod("affinity", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_DATA_STREAMER = JniMethod("dataStreamer", "(Ljava/lang/String;Z)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_TRANSACTIONS = JniMethod("transactions", "()Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_COMPUTE = JniMethod("compute", "(Lorg/apache/ignite/internal/processors/platform/PlatformTarget;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_MESSAGE = JniMethod("message", "(Lorg/apache/ignite/internal/processors/platform/PlatformTarget;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_EVENTS = JniMethod("events", "(Lorg/apache/ignite/internal/processors/platform/PlatformTarget;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_SERVICES = JniMethod("services", "(Lorg/apache/ignite/internal/processors/platform/PlatformTarget;)Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            JniMethod M_PLATFORM_PROCESSOR_EXTENSIONS = JniMethod("extensions", "()Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);
            
            const char* C_PLATFORM_TARGET = "org/apache/ignite/internal/processors/platform/PlatformTarget";
            JniMethod M_PLATFORM_TARGET_IN_STREAM_OUT_LONG = JniMethod("inStreamOutLong", "(IJ)J", false);
            JniMethod M_PLATFORM_TARGET_IN_STREAM_OUT_OBJECT = JniMethod("inStreamOutObject", "(IJ)Ljava/lang/Object;", false);
            JniMethod M_PLATFORM_TARGET_IN_STREAM_OUT_STREAM = JniMethod("inStreamOutStream", "(IJJ)V", false);
            JniMethod M_PLATFORM_TARGET_IN_OBJECT_STREAM_OUT_STREAM = JniMethod("inObjectStreamOutStream", "(ILjava/lang/Object;JJ)V", false);
            JniMethod M_PLATFORM_TARGET_OUT_LONG = JniMethod("outLong", "(I)J", false);
            JniMethod M_PLATFORM_TARGET_OUT_STREAM = JniMethod("outStream", "(IJ)V", false);
            JniMethod M_PLATFORM_TARGET_OUT_OBJECT = JniMethod("outObject", "(I)Ljava/lang/Object;", false);
            JniMethod M_PLATFORM_TARGET_LISTEN_FUTURE = JniMethod("listenFuture", "(JI)V", false);
            JniMethod M_PLATFORM_TARGET_LISTEN_FOR_OPERATION = JniMethod("listenFutureForOperation", "(JII)V", false);

            const char* C_PLATFORM_CLUSTER_GRP = "org/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup";
            JniMethod M_PLATFORM_CLUSTER_GRP_FOR_OTHERS = JniMethod("forOthers", "(Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;)Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;", false);
            JniMethod M_PLATFORM_CLUSTER_GRP_FOR_REMOTES = JniMethod("forRemotes", "()Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;", false);
            JniMethod M_PLATFORM_CLUSTER_GRP_FOR_DAEMONS = JniMethod("forDaemons", "()Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;", false);
            JniMethod M_PLATFORM_CLUSTER_GRP_FOR_RANDOM = JniMethod("forRandom", "()Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;", false);
            JniMethod M_PLATFORM_CLUSTER_GRP_FOR_OLDEST = JniMethod("forOldest", "()Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;", false);
            JniMethod M_PLATFORM_CLUSTER_GRP_FOR_YOUNGEST = JniMethod("forYoungest", "()Lorg/apache/ignite/internal/processors/platform/cluster/PlatformClusterGroup;", false);
            JniMethod M_PLATFORM_CLUSTER_GRP_RESET_METRICS = JniMethod("resetMetrics", "()V", false);
            
            const char* C_PLATFORM_MESSAGING = "org/apache/ignite/internal/processors/platform/messaging/PlatformMessaging";
            JniMethod M_PLATFORM_MESSAGING_WITH_ASYNC = JniMethod("withAsync", "()Lorg/apache/ignite/internal/processors/platform/messaging/PlatformMessaging;", false);

            const char* C_PLATFORM_COMPUTE = "org/apache/ignite/internal/processors/platform/compute/PlatformCompute";
            JniMethod M_PLATFORM_COMPUTE_WITH_NO_FAILOVER = JniMethod("withNoFailover", "()V", false);
            JniMethod M_PLATFORM_COMPUTE_WITH_TIMEOUT = JniMethod("withTimeout", "(J)V", false);
            JniMethod M_PLATFORM_COMPUTE_EXECUTE_NATIVE = JniMethod("executeNative", "(JJ)V", false);

            const char* C_PLATFORM_CACHE = "org/apache/ignite/internal/processors/platform/cache/PlatformCache";
            JniMethod M_PLATFORM_CACHE_WITH_SKIP_STORE = JniMethod("withSkipStore", "()Lorg/apache/ignite/internal/processors/platform/cache/PlatformCache;", false);
            JniMethod M_PLATFORM_CACHE_WITH_NO_RETRIES = JniMethod("withNoRetries", "()Lorg/apache/ignite/internal/processors/platform/cache/PlatformCache;", false);
            JniMethod M_PLATFORM_CACHE_WITH_EXPIRY_PLC = JniMethod("withExpiryPolicy", "(JJJ)Lorg/apache/ignite/internal/processors/platform/cache/PlatformCache;", false);
            JniMethod M_PLATFORM_CACHE_WITH_ASYNC = JniMethod("withAsync", "()Lorg/apache/ignite/internal/processors/platform/cache/PlatformCache;", false);
            JniMethod M_PLATFORM_CACHE_WITH_KEEP_PORTABLE = JniMethod("withKeepPortable", "()Lorg/apache/ignite/internal/processors/platform/cache/PlatformCache;", false);
            JniMethod M_PLATFORM_CACHE_CLEAR = JniMethod("clear", "()V", false);
            JniMethod M_PLATFORM_CACHE_REMOVE_ALL = JniMethod("removeAll", "()V", false);
            JniMethod M_PLATFORM_CACHE_ITERATOR = JniMethod("iterator", "()Lorg/apache/ignite/internal/processors/platform/cache/PlatformCacheIterator;", false);
            JniMethod M_PLATFORM_CACHE_LOCAL_ITERATOR = JniMethod("localIterator", "(I)Lorg/apache/ignite/internal/processors/platform/cache/PlatformCacheIterator;", false);
            JniMethod M_PLATFORM_CACHE_ENTER_LOCK = JniMethod("enterLock", "(J)V", false);
            JniMethod M_PLATFORM_CACHE_EXIT_LOCK = JniMethod("exitLock", "(J)V", false);
            JniMethod M_PLATFORM_CACHE_TRY_ENTER_LOCK = JniMethod("tryEnterLock", "(JJ)Z", false);
            JniMethod M_PLATFORM_CACHE_CLOSE_LOCK = JniMethod("closeLock", "(J)V", false);
            JniMethod M_PLATFORM_CACHE_REBALANCE = JniMethod("rebalance", "(J)V", false);
            JniMethod M_PLATFORM_CACHE_SIZE = JniMethod("size", "(IZ)I", false);

            const char* C_PLATFORM_AFFINITY = "org/apache/ignite/internal/processors/platform/cache/affinity/PlatformAffinity";
            JniMethod C_PLATFORM_AFFINITY_PARTITIONS = JniMethod("partitions", "()I", false);

            const char* C_PLATFORM_DATA_STREAMER = "org/apache/ignite/internal/processors/platform/datastreamer/PlatformDataStreamer";
            JniMethod M_PLATFORM_DATA_STREAMER_LISTEN_TOPOLOGY = JniMethod("listenTopology", "(J)V", false);
            JniMethod M_PLATFORM_DATA_STREAMER_GET_ALLOW_OVERWRITE = JniMethod("allowOverwrite", "()Z", false);
            JniMethod M_PLATFORM_DATA_STREAMER_SET_ALLOW_OVERWRITE = JniMethod("allowOverwrite", "(Z)V", false);
            JniMethod M_PLATFORM_DATA_STREAMER_GET_SKIP_STORE = JniMethod("skipStore", "()Z", false);
            JniMethod M_PLATFORM_DATA_STREAMER_SET_SKIP_STORE = JniMethod("skipStore", "(Z)V", false);
            JniMethod M_PLATFORM_DATA_STREAMER_GET_PER_NODE_BUFFER_SIZE = JniMethod("perNodeBufferSize", "()I", false);
            JniMethod M_PLATFORM_DATA_STREAMER_SET_PER_NODE_BUFFER_SIZE = JniMethod("perNodeBufferSize", "(I)V", false);
            JniMethod M_PLATFORM_DATA_STREAMER_GET_PER_NODE_PARALLEL_OPS = JniMethod("perNodeParallelOperations", "()I", false);
            JniMethod M_PLATFORM_DATA_STREAMER_SET_PER_NODE_PARALLEL_OPS = JniMethod("perNodeParallelOperations", "(I)V", false);

            const char* C_PLATFORM_TRANSACTIONS = "org/apache/ignite/internal/processors/platform/transactions/PlatformTransactions";
            JniMethod M_PLATFORM_TRANSACTIONS_TX_START = JniMethod("txStart", "(IIJI)J", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_COMMIT = JniMethod("txCommit", "(J)I", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_ROLLBACK = JniMethod("txRollback", "(J)I", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_COMMIT_ASYNC = JniMethod("txCommitAsync", "(JJ)V", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_ROLLBACK_ASYNC = JniMethod("txRollbackAsync", "(JJ)V", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_STATE = JniMethod("txState", "(J)I", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_SET_ROLLBACK_ONLY = JniMethod("txSetRollbackOnly", "(J)Z", false);
            JniMethod M_PLATFORM_TRANSACTIONS_TX_CLOSE = JniMethod("txClose", "(J)I", false);
            JniMethod M_PLATFORM_TRANSACTIONS_RESET_METRICS = JniMethod("resetMetrics", "()V", false);

            const char* C_PLATFORM_CACHE_STORE_CALLBACK = "org/apache/ignite/internal/processors/platform/cache/store/PlatformCacheStoreCallback";
            JniMethod M_PLATFORM_CACHE_STORE_CALLBACK_INVOKE = JniMethod("invoke", "(J)V", false);

            const char* C_PLATFORM_CALLBACK_UTILS = "org/apache/ignite/internal/processors/platform/callback/PlatformCallbackUtils";

            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_CREATE = JniMethod("cacheStoreCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_INVOKE = JniMethod("cacheStoreInvoke", "(JJJLjava/lang/Object;)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_DESTROY = JniMethod("cacheStoreDestroy", "(JJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_SESSION_CREATE = JniMethod("cacheStoreSessionCreate", "(JJ)J", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_ENTRY_FILTER_CREATE = JniMethod("cacheEntryFilterCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_ENTRY_FILTER_APPLY = JniMethod("cacheEntryFilterApply", "(JJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_ENTRY_FILTER_DESTROY = JniMethod("cacheEntryFilterDestroy", "(JJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_INVOKE = JniMethod("cacheInvoke", "(JJJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_MAP = JniMethod("computeTaskMap", "(JJJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_JOB_RESULT = JniMethod("computeTaskJobResult", "(JJJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_REDUCE = JniMethod("computeTaskReduce", "(JJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_COMPLETE = JniMethod("computeTaskComplete", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_SERIALIZE = JniMethod("computeJobSerialize", "(JJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_CREATE = JniMethod("computeJobCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_EXECUTE = JniMethod("computeJobExecute", "(JJIJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_DESTROY = JniMethod("computeJobDestroy", "(JJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_CANCEL = JniMethod("computeJobCancel", "(JJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_LSNR_APPLY = JniMethod("continuousQueryListenerApply", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_FILTER_CREATE = JniMethod("continuousQueryFilterCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_FILTER_EVAL = JniMethod("continuousQueryFilterApply", "(JJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_FILTER_RELEASE = JniMethod("continuousQueryFilterRelease", "(JJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_DATA_STREAMER_TOPOLOGY_UPDATE = JniMethod("dataStreamerTopologyUpdate", "(JJJI)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_DATA_STREAMER_STREAM_RECEIVER_INVOKE = JniMethod("dataStreamerStreamReceiverInvoke", "(JJLjava/lang/Object;JZ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_BYTE_RES = JniMethod("futureByteResult", "(JJI)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_BOOL_RES = JniMethod("futureBoolResult", "(JJI)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_SHORT_RES = JniMethod("futureShortResult", "(JJI)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_CHAR_RES = JniMethod("futureCharResult", "(JJI)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_INT_RES = JniMethod("futureIntResult", "(JJI)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_FLOAT_RES = JniMethod("futureFloatResult", "(JJF)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_LONG_RES = JniMethod("futureLongResult", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_DOUBLE_RES = JniMethod("futureDoubleResult", "(JJD)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_OBJ_RES = JniMethod("futureObjectResult", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_NULL_RES = JniMethod("futureNullResult", "(JJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_FUTURE_ERR = JniMethod("futureError", "(JJJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_LIFECYCLE_EVENT = JniMethod("lifecycleEvent", "(JJI)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_MESSAGING_FILTER_CREATE = JniMethod("messagingFilterCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_MESSAGING_FILTER_APPLY = JniMethod("messagingFilterApply", "(JJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_MESSAGING_FILTER_DESTROY = JniMethod("messagingFilterDestroy", "(JJ)V", true);
            
            JniMethod M_PLATFORM_CALLBACK_UTILS_EVENT_FILTER_CREATE = JniMethod("eventFilterCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_EVENT_FILTER_APPLY = JniMethod("eventFilterApply", "(JJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_EVENT_FILTER_DESTROY = JniMethod("eventFilterDestroy", "(JJ)V", true);
            
            JniMethod M_PLATFORM_CALLBACK_UTILS_SERVICE_INIT = JniMethod("serviceInit", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_SERVICE_EXECUTE = JniMethod("serviceExecute", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_SERVICE_CANCEL = JniMethod("serviceCancel", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_SERVICE_INVOKE_METHOD = JniMethod("serviceInvokeMethod", "(JJJJ)V", true);
			
            JniMethod M_PLATFORM_CALLBACK_UTILS_CLUSTER_NODE_FILTER_APPLY = JniMethod("clusterNodeFilterApply", "(JJ)I", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_NODE_INFO = JniMethod("nodeInfo", "(JJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_MEMORY_REALLOCATE = JniMethod("memoryReallocate", "(JJI)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_ON_START = JniMethod("onStart", "(JLjava/lang/Object;J)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_ON_STOP = JniMethod("onStop", "(J)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_EXTENSION_CALLBACK_IN_LONG_OUT_LONG = JniMethod("extensionCallbackInLongOutLong", "(JIJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_EXTENSION_CALLBACK_IN_LONG_LONG_OUT_LONG = JniMethod("extensionCallbackInLongLongOutLong", "(JIJJ)J", true);
            
            const char* C_PLATFORM_UTILS = "org/apache/ignite/internal/processors/platform/utils/PlatformUtils";
            JniMethod M_PLATFORM_UTILS_REALLOC = JniMethod("reallocate", "(JI)V", true);
            JniMethod M_PLATFORM_UTILS_ERR_DATA = JniMethod("errorData", "(Ljava/lang/Throwable;)[B", true);

            const char* C_PLATFORM_IGNITION = "org/apache/ignite/internal/processors/platform/PlatformIgnition";
            JniMethod M_PLATFORM_IGNITION_START = JniMethod("start", "(Ljava/lang/String;Ljava/lang/String;IJJ)Lorg/apache/ignite/internal/processors/platform/PlatformProcessor;", true);
            JniMethod M_PLATFORM_IGNITION_INSTANCE = JniMethod("instance", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformProcessor;", true);
            JniMethod M_PLATFORM_IGNITION_ENVIRONMENT_POINTER = JniMethod("environmentPointer", "(Ljava/lang/String;)J", true);
            JniMethod M_PLATFORM_IGNITION_STOP = JniMethod("stop", "(Ljava/lang/String;Z)Z", true);
            JniMethod M_PLATFORM_IGNITION_STOP_ALL = JniMethod("stopAll", "(Z)V", true);
            
            const char* C_PLATFORM_ABSTRACT_QRY_CURSOR = "org/apache/ignite/internal/processors/platform/cache/query/PlatformAbstractQueryCursor";
            JniMethod M_PLATFORM_ABSTRACT_QRY_CURSOR_ITER = JniMethod("iterator", "()V", false);
            JniMethod M_PLATFORM_ABSTRACT_QRY_CURSOR_ITER_HAS_NEXT = JniMethod("iteratorHasNext", "()Z", false);
            JniMethod M_PLATFORM_ABSTRACT_QRY_CURSOR_CLOSE = JniMethod("close", "()V", false);

            const char* C_PLATFORM_CONT_QRY = "org/apache/ignite/internal/processors/platform/cache/query/PlatformContinuousQuery";
            JniMethod M_PLATFORM_CONT_QRY_CLOSE = JniMethod("close", "()V", false);
            JniMethod M_PLATFORM_CONT_QRY_GET_INITIAL_QUERY_CURSOR = JniMethod("getInitialQueryCursor", "()Lorg/apache/ignite/internal/processors/platform/PlatformTarget;", false);

            const char* C_PLATFORM_EVENTS = "org/apache/ignite/internal/processors/platform/events/PlatformEvents";
            JniMethod M_PLATFORM_EVENTS_WITH_ASYNC = JniMethod("withAsync", "()Lorg/apache/ignite/internal/processors/platform/events/PlatformEvents;", false);
            JniMethod M_PLATFORM_EVENTS_STOP_LOCAL_LISTEN = JniMethod("stopLocalListen", "(J)Z", false);
            JniMethod M_PLATFORM_EVENTS_LOCAL_LISTEN = JniMethod("localListen", "(JI)V", false);
            JniMethod M_PLATFORM_EVENTS_IS_ENABLED = JniMethod("isEnabled", "(I)Z", false);
            
            const char* C_PLATFORM_SERVICES = "org/apache/ignite/internal/processors/platform/services/PlatformServices";
			JniMethod M_PLATFORM_SERVICES_WITH_ASYNC = JniMethod("withAsync", "()Lorg/apache/ignite/internal/processors/platform/services/PlatformServices;", false);
			JniMethod M_PLATFORM_SERVICES_WITH_SERVER_KEEP_PORTABLE = JniMethod("withServerKeepPortable", "()Lorg/apache/ignite/internal/processors/platform/services/PlatformServices;", false);
			JniMethod M_PLATFORM_SERVICES_CANCEL = JniMethod("cancel", "(Ljava/lang/String;)V", false);
			JniMethod M_PLATFORM_SERVICES_CANCEL_ALL = JniMethod("cancelAll", "()V", false);
			JniMethod M_PLATFORM_SERVICES_SERVICE_PROXY = JniMethod("dotNetServiceProxy", "(Ljava/lang/String;Z)Ljava/lang/Object;", false);

            /* STATIC STATE. */
            gcc::CriticalSection JVM_LOCK;
            JniJvm JVM;
            bool PRINT_EXCEPTION = false;

            /* HELPER METHODS. */

            /*
             * Throw exception to Java in case of missing callback pointer. It means that callback is not implemented in
             * native platform and Java -> platform operation cannot proceede further. As JniContext is not available at
             * this point, we have to obtain exception details from scratch. This is not critical from performance
             * perspective because missing handler usually denotes fatal condition.
             *
             * @param env JNI environment.
             */
            int ThrowOnMissingHandler(JNIEnv* env)
            {
                jclass cls = env->FindClass(C_PLATFORM_NO_CALLBACK_EXCEPTION);

                env->ThrowNew(cls, "Callback handler is not set in native platform.");

                return 0;
            }

            char* StringToChars(JNIEnv* env, jstring str, int* len) {
                if (!str) {
                    *len = 0;
                    return NULL;
                }

                const char* strChars = env->GetStringUTFChars(str, 0);
                const int strCharsLen = env->GetStringUTFLength(str);

                char* strChars0 = new char[strCharsLen + 1];
                std::strcpy(strChars0, strChars);
                *(strChars0 + strCharsLen) = 0;

                env->ReleaseStringUTFChars(str, strChars);

                if (len)
                    *len = strCharsLen;

                return strChars0;
            }

            std::string JavaStringToCString(JNIEnv* env, jstring str, int* len)
            {
                char* resChars = StringToChars(env, str, len);

                if (resChars)
                {
                    std::string res = std::string(resChars, *len);

                    delete[] resChars;
                    
                    return res;
                }
                else
                    return std::string();
            }

            jclass FindClass(JNIEnv* env, const char *name) {
                jclass res = env->FindClass(name);

                if (!res)
                    throw JvmException();

                jclass res0 = static_cast<jclass>(env->NewGlobalRef(res));

                env->DeleteLocalRef(res);

                return res0;
            }

            void DeleteClass(JNIEnv* env, jclass cls) {
                if (cls)
                    env->DeleteGlobalRef(cls);
            }

            void CheckClass(JNIEnv* env, const char *name)
            {
                jclass res = env->FindClass(name);

                if (!res)
                    throw JvmException();
            }

            jmethodID FindMethod(JNIEnv* env, jclass cls, JniMethod mthd) {
                jmethodID mthd0 = mthd.isStatic ?
                    env->GetStaticMethodID(cls, mthd.name, mthd.sign) : env->GetMethodID(cls, mthd.name, mthd.sign);

                if (!mthd0)
                    throw JvmException();

                return mthd0;
            }

            void AddNativeMethod(JNINativeMethod* mthd, JniMethod jniMthd, void* fnPtr) {
                mthd->name = jniMthd.name;
                mthd->signature = jniMthd.sign;
                mthd->fnPtr = fnPtr;
            }

            void JniJavaMembers::Initialize(JNIEnv* env) {
                c_Class = FindClass(env, C_CLASS);
                m_Class_getName = FindMethod(env, c_Class, M_CLASS_GET_NAME);

                c_Throwable = FindClass(env, C_THROWABLE);
                m_Throwable_getMessage = FindMethod(env, c_Throwable, M_THROWABLE_GET_MESSAGE);
                m_Throwable_printStackTrace = FindMethod(env, c_Throwable, M_THROWABLE_PRINT_STACK_TRACE);
            }

            void JniJavaMembers::Destroy(JNIEnv* env) {
                DeleteClass(env, c_Class);
                DeleteClass(env, c_Throwable);
            }

            bool JniJavaMembers::WriteErrorInfo(JNIEnv* env, char** errClsName, int* errClsNameLen, char** errMsg, int* errMsgLen) {
                if (env && env->ExceptionCheck()) {
                    if (m_Class_getName && m_Throwable_getMessage) {
                        jthrowable err = env->ExceptionOccurred();

                        env->ExceptionClear();

                        jclass errCls = env->GetObjectClass(err);

                        jstring clsName = static_cast<jstring>(env->CallObjectMethod(errCls, m_Class_getName));
                        *errClsName = StringToChars(env, clsName, errClsNameLen);

                        jstring msg = static_cast<jstring>(env->CallObjectMethod(err, m_Throwable_getMessage));
                        *errMsg = StringToChars(env, msg, errMsgLen);

                        if (errCls)
                            env->DeleteLocalRef(errCls);

                        if (clsName)
                            env->DeleteLocalRef(clsName);

                        if (msg)
                            env->DeleteLocalRef(msg);

                        return true;
                    }
                    else {
                        env->ExceptionClear();
                    }
                }

                return false;
            }

            void JniMembers::Initialize(JNIEnv* env) {
                c_PlatformAbstractQryCursor = FindClass(env, C_PLATFORM_ABSTRACT_QRY_CURSOR);
                m_PlatformAbstractQryCursor_iter = FindMethod(env, c_PlatformAbstractQryCursor, M_PLATFORM_ABSTRACT_QRY_CURSOR_ITER);
                m_PlatformAbstractQryCursor_iterHasNext = FindMethod(env, c_PlatformAbstractQryCursor, M_PLATFORM_ABSTRACT_QRY_CURSOR_ITER_HAS_NEXT);
                m_PlatformAbstractQryCursor_close = FindMethod(env, c_PlatformAbstractQryCursor, M_PLATFORM_ABSTRACT_QRY_CURSOR_CLOSE);

                c_PlatformAffinity = FindClass(env, C_PLATFORM_AFFINITY);
                m_PlatformAffinity_partitions = FindMethod(env, c_PlatformAffinity, C_PLATFORM_AFFINITY_PARTITIONS);

                c_PlatformCache = FindClass(env, C_PLATFORM_CACHE);
                m_PlatformCache_withSkipStore = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_WITH_SKIP_STORE);
                m_PlatformCache_withNoRetries = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_WITH_NO_RETRIES);
                m_PlatformCache_withExpiryPolicy = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_WITH_EXPIRY_PLC);
                m_PlatformCache_withAsync = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_WITH_ASYNC);
                m_PlatformCache_withKeepPortable = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_WITH_KEEP_PORTABLE);
                m_PlatformCache_clear = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_CLEAR);
                m_PlatformCache_removeAll = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_REMOVE_ALL);
                m_PlatformCache_iterator = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_ITERATOR);
                m_PlatformCache_localIterator = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_LOCAL_ITERATOR);
                m_PlatformCache_enterLock = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_ENTER_LOCK);
                m_PlatformCache_exitLock = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_EXIT_LOCK);
                m_PlatformCache_tryEnterLock = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_TRY_ENTER_LOCK);
                m_PlatformCache_closeLock = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_CLOSE_LOCK);
                m_PlatformCache_rebalance = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_REBALANCE);
                m_PlatformCache_size = FindMethod(env, c_PlatformCache, M_PLATFORM_CACHE_SIZE);

                c_PlatformCacheStoreCallback = FindClass(env, C_PLATFORM_CACHE_STORE_CALLBACK);
                m_PlatformCacheStoreCallback_invoke = FindMethod(env, c_PlatformCacheStoreCallback, M_PLATFORM_CACHE_STORE_CALLBACK_INVOKE);

                c_IgniteException = FindClass(env, C_IGNITE_EXCEPTION);

                c_PlatformClusterGroup = FindClass(env, C_PLATFORM_CLUSTER_GRP);
                m_PlatformClusterGroup_forOthers = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_FOR_OTHERS);
                m_PlatformClusterGroup_forRemotes = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_FOR_REMOTES);
                m_PlatformClusterGroup_forDaemons = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_FOR_DAEMONS);
                m_PlatformClusterGroup_forRandom = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_FOR_RANDOM);
                m_PlatformClusterGroup_forOldest = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_FOR_OLDEST);
                m_PlatformClusterGroup_forYoungest = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_FOR_YOUNGEST);
                m_PlatformClusterGroup_resetMetrics = FindMethod(env, c_PlatformClusterGroup, M_PLATFORM_CLUSTER_GRP_RESET_METRICS);

                c_PlatformCompute = FindClass(env, C_PLATFORM_COMPUTE);
                m_PlatformCompute_withNoFailover = FindMethod(env, c_PlatformCompute, M_PLATFORM_COMPUTE_WITH_NO_FAILOVER);
                m_PlatformCompute_withTimeout = FindMethod(env, c_PlatformCompute, M_PLATFORM_COMPUTE_WITH_TIMEOUT);
                m_PlatformCompute_executeNative = FindMethod(env, c_PlatformCompute, M_PLATFORM_COMPUTE_EXECUTE_NATIVE);

                c_PlatformContinuousQuery = FindClass(env, C_PLATFORM_CONT_QRY);
                m_PlatformContinuousQuery_close = FindMethod(env, c_PlatformContinuousQuery, M_PLATFORM_CONT_QRY_CLOSE);
                m_PlatformContinuousQuery_getInitialQueryCursor = FindMethod(env, c_PlatformContinuousQuery, M_PLATFORM_CONT_QRY_GET_INITIAL_QUERY_CURSOR);

                c_PlatformDataStreamer = FindClass(env, C_PLATFORM_DATA_STREAMER);
                m_PlatformDataStreamer_listenTopology = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_LISTEN_TOPOLOGY);
                m_PlatformDataStreamer_getAllowOverwrite = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_GET_ALLOW_OVERWRITE);
                m_PlatformDataStreamer_setAllowOverwrite = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_SET_ALLOW_OVERWRITE);
                m_PlatformDataStreamer_getSkipStore = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_GET_SKIP_STORE);
                m_PlatformDataStreamer_setSkipStore = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_SET_SKIP_STORE);
                m_PlatformDataStreamer_getPerNodeBufSize = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_GET_PER_NODE_BUFFER_SIZE);
                m_PlatformDataStreamer_setPerNodeBufSize = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_SET_PER_NODE_BUFFER_SIZE);
                m_PlatformDataStreamer_getPerNodeParallelOps = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_GET_PER_NODE_PARALLEL_OPS);
                m_PlatformDataStreamer_setPerNodeParallelOps = FindMethod(env, c_PlatformDataStreamer, M_PLATFORM_DATA_STREAMER_SET_PER_NODE_PARALLEL_OPS);
                
                c_PlatformEvents = FindClass(env, C_PLATFORM_EVENTS);
                m_PlatformEvents_withAsync = FindMethod(env, c_PlatformEvents, M_PLATFORM_EVENTS_WITH_ASYNC);
                m_PlatformEvents_stopLocalListen = FindMethod(env, c_PlatformEvents, M_PLATFORM_EVENTS_STOP_LOCAL_LISTEN);
                m_PlatformEvents_localListen = FindMethod(env, c_PlatformEvents, M_PLATFORM_EVENTS_LOCAL_LISTEN);
                m_PlatformEvents_isEnabled = FindMethod(env, c_PlatformEvents, M_PLATFORM_EVENTS_IS_ENABLED);
                
				c_PlatformServices = FindClass(env, C_PLATFORM_SERVICES);
				m_PlatformServices_withAsync = FindMethod(env, c_PlatformServices, M_PLATFORM_SERVICES_WITH_ASYNC);
				m_PlatformServices_withServerKeepPortable = FindMethod(env, c_PlatformServices, M_PLATFORM_SERVICES_WITH_SERVER_KEEP_PORTABLE);
				m_PlatformServices_cancel = FindMethod(env, c_PlatformServices, M_PLATFORM_SERVICES_CANCEL);
				m_PlatformServices_cancelAll = FindMethod(env, c_PlatformServices, M_PLATFORM_SERVICES_CANCEL_ALL);
				m_PlatformServices_serviceProxy = FindMethod(env, c_PlatformServices, M_PLATFORM_SERVICES_SERVICE_PROXY);

                c_PlatformIgnition = FindClass(env, C_PLATFORM_IGNITION);
                m_PlatformIgnition_start = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_START);
                m_PlatformIgnition_instance = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_INSTANCE);
                m_PlatformIgnition_environmentPointer = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_ENVIRONMENT_POINTER);
                m_PlatformIgnition_stop = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_STOP);
                m_PlatformIgnition_stopAll = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_STOP_ALL);

                c_PlatformMessaging = FindClass(env, C_PLATFORM_MESSAGING);
                m_PlatformMessaging_withAsync = FindMethod(env, c_PlatformMessaging, M_PLATFORM_MESSAGING_WITH_ASYNC);

                c_PlatformProcessor = FindClass(env, C_PLATFORM_PROCESSOR);
                m_PlatformProcessor_releaseStart = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_RELEASE_START);
                m_PlatformProcessor_cache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_CACHE);
                m_PlatformProcessor_createCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_CREATE_CACHE);
                m_PlatformProcessor_getOrCreateCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_GET_OR_CREATE_CACHE);
                m_PlatformProcessor_affinity = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_AFFINITY);
                m_PlatformProcessor_dataStreamer = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_DATA_STREAMER);
                m_PlatformProcessor_transactions = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_TRANSACTIONS);
                m_PlatformProcessor_projection = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_PROJECTION);
                m_PlatformProcessor_compute = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_COMPUTE);
                m_PlatformProcessor_message = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_MESSAGE);
                m_PlatformProcessor_events = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_EVENTS);
                m_PlatformProcessor_services = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_SERVICES);
                m_PlatformProcessor_extensions = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_EXTENSIONS);

                c_PlatformTarget = FindClass(env, C_PLATFORM_TARGET);
                m_PlatformTarget_inStreamOutLong = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_STREAM_OUT_LONG);
                m_PlatformTarget_inStreamOutObject = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_STREAM_OUT_OBJECT);
                m_PlatformTarget_outLong = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_OUT_LONG);
                m_PlatformTarget_outStream = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_OUT_STREAM);
                m_PlatformTarget_outObject = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_OUT_OBJECT);
                m_PlatformTarget_inStreamOutStream = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_STREAM_OUT_STREAM);
                m_PlatformTarget_inObjectStreamOutStream = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_OBJECT_STREAM_OUT_STREAM);
                m_PlatformTarget_listenFuture = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_LISTEN_FUTURE);
                m_PlatformTarget_listenFutureForOperation = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_LISTEN_FOR_OPERATION);

                c_PlatformTransactions = FindClass(env, C_PLATFORM_TRANSACTIONS);
                m_PlatformTransactions_txStart = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_START);
                m_PlatformTransactions_txCommit = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_COMMIT);
                m_PlatformTransactions_txRollback = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_ROLLBACK);
                m_PlatformTransactions_txCommitAsync = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_COMMIT_ASYNC);
                m_PlatformTransactions_txRollbackAsync = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_ROLLBACK_ASYNC);
                m_PlatformTransactions_txState = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_STATE);
                m_PlatformTransactions_txSetRollbackOnly = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_SET_ROLLBACK_ONLY);
                m_PlatformTransactions_txClose = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_TX_CLOSE);
                m_PlatformTransactions_resetMetrics = FindMethod(env, c_PlatformTransactions, M_PLATFORM_TRANSACTIONS_RESET_METRICS);

                c_PlatformUtils = FindClass(env, C_PLATFORM_UTILS);
                m_PlatformUtils_reallocate = FindMethod(env, c_PlatformUtils, M_PLATFORM_UTILS_REALLOC);
                m_PlatformUtils_errData = FindMethod(env, c_PlatformUtils, M_PLATFORM_UTILS_ERR_DATA);

                // Find utility classes which are not used from context, but are still required in other places.
                CheckClass(env, C_PLATFORM_NO_CALLBACK_EXCEPTION);
            }

            void JniMembers::Destroy(JNIEnv* env) {
                DeleteClass(env, c_PlatformAbstractQryCursor);
                DeleteClass(env, c_PlatformAffinity);
                DeleteClass(env, c_PlatformCache);
                DeleteClass(env, c_PlatformCacheStoreCallback);
                DeleteClass(env, c_IgniteException);
                DeleteClass(env, c_PlatformClusterGroup);
                DeleteClass(env, c_PlatformCompute);
                DeleteClass(env, c_PlatformContinuousQuery);
                DeleteClass(env, c_PlatformDataStreamer);
                DeleteClass(env, c_PlatformEvents);
                DeleteClass(env, c_PlatformIgnition);
                DeleteClass(env, c_PlatformMessaging);
                DeleteClass(env, c_PlatformProcessor);
                DeleteClass(env, c_PlatformTarget);
                DeleteClass(env, c_PlatformTransactions);
                DeleteClass(env, c_PlatformUtils);
            }

            JniJvm::JniJvm() : jvm(NULL), javaMembers(JniJavaMembers()), members(JniMembers())
            {
                // No-op.
            }

            JniJvm::JniJvm(JavaVM* jvm, JniJavaMembers javaMembers, JniMembers members) : 
                jvm(jvm), javaMembers(javaMembers), members(members)
            {
                // No-op.
            }

            JavaVM* JniJvm::GetJvm()
            {
                return jvm;
            }

            JniJavaMembers& JniJvm::GetJavaMembers()
            {
                return javaMembers;
            }

            JniMembers& JniJvm::GetMembers()
            {
                return members;
            }

            /*
             * Create JVM.
             */
            void CreateJvm(char** opts, int optsLen, JavaVM** jvm, JNIEnv** env) {
                JavaVMOption* opts0 = new JavaVMOption[optsLen];

                for (int i = 0; i < optsLen; i++)
                    opts0[i].optionString = *(opts + i);

                JavaVMInitArgs args;

                args.version = JNI_VERSION_1_6;
                args.nOptions = optsLen;
                args.options = opts0;
                args.ignoreUnrecognized = 0;

                jint res = JNI_CreateJavaVM(jvm, reinterpret_cast<void**>(env), &args);

                delete[] opts0;

                if (res != JNI_OK)
                    throw JvmException();
            }

            void RegisterNatives(JNIEnv* env) {
                {
					JNINativeMethod methods[52];

                    int idx = 0;

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_CREATE, reinterpret_cast<void*>(JniCacheStoreCreate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_INVOKE, reinterpret_cast<void*>(JniCacheStoreInvoke));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_DESTROY, reinterpret_cast<void*>(JniCacheStoreDestroy));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_SESSION_CREATE, reinterpret_cast<void*>(JniCacheStoreSessionCreate));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_ENTRY_FILTER_CREATE, reinterpret_cast<void*>(JniCacheEntryFilterCreate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_ENTRY_FILTER_APPLY, reinterpret_cast<void*>(JniCacheEntryFilterApply));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_ENTRY_FILTER_DESTROY, reinterpret_cast<void*>(JniCacheEntryFilterDestroy));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CACHE_INVOKE, reinterpret_cast<void*>(JniCacheInvoke));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_MAP, reinterpret_cast<void*>(JniComputeTaskMap));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_JOB_RESULT, reinterpret_cast<void*>(JniComputeTaskJobResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_REDUCE, reinterpret_cast<void*>(JniComputeTaskReduce));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_TASK_COMPLETE, reinterpret_cast<void*>(JniComputeTaskComplete));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_SERIALIZE, reinterpret_cast<void*>(JniComputeJobSerialize));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_CREATE, reinterpret_cast<void*>(JniComputeJobCreate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_EXECUTE, reinterpret_cast<void*>(JniComputeJobExecute));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_DESTROY, reinterpret_cast<void*>(JniComputeJobDestroy));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_COMPUTE_JOB_CANCEL, reinterpret_cast<void*>(JniComputeJobCancel));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_LSNR_APPLY, reinterpret_cast<void*>(JniContinuousQueryListenerApply));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_FILTER_CREATE, reinterpret_cast<void*>(JniContinuousQueryFilterCreate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_FILTER_EVAL, reinterpret_cast<void*>(JniContinuousQueryFilterApply));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CONTINUOUS_QUERY_FILTER_RELEASE, reinterpret_cast<void*>(JniContinuousQueryFilterRelease));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_DATA_STREAMER_TOPOLOGY_UPDATE, reinterpret_cast<void*>(JniDataStreamerTopologyUpdate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_DATA_STREAMER_STREAM_RECEIVER_INVOKE, reinterpret_cast<void*>(JniDataStreamerStreamReceiverInvoke));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_BYTE_RES, reinterpret_cast<void*>(JniFutureByteResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_BOOL_RES, reinterpret_cast<void*>(JniFutureBoolResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_SHORT_RES, reinterpret_cast<void*>(JniFutureShortResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_CHAR_RES, reinterpret_cast<void*>(JniFutureCharResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_INT_RES, reinterpret_cast<void*>(JniFutureIntResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_FLOAT_RES, reinterpret_cast<void*>(JniFutureFloatResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_LONG_RES, reinterpret_cast<void*>(JniFutureLongResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_DOUBLE_RES, reinterpret_cast<void*>(JniFutureDoubleResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_OBJ_RES, reinterpret_cast<void*>(JniFutureObjectResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_NULL_RES, reinterpret_cast<void*>(JniFutureNullResult));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_FUTURE_ERR, reinterpret_cast<void*>(JniFutureError));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_LIFECYCLE_EVENT, reinterpret_cast<void*>(JniLifecycleEvent));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_MESSAGING_FILTER_CREATE, reinterpret_cast<void*>(JniMessagingFilterCreate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_MESSAGING_FILTER_APPLY, reinterpret_cast<void*>(JniMessagingFilterApply));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_MESSAGING_FILTER_DESTROY, reinterpret_cast<void*>(JniMessagingFilterDestroy));
                    
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_EVENT_FILTER_CREATE, reinterpret_cast<void*>(JniEventFilterCreate));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_EVENT_FILTER_APPLY, reinterpret_cast<void*>(JniEventFilterApply));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_EVENT_FILTER_DESTROY, reinterpret_cast<void*>(JniEventFilterDestroy));
                    
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_SERVICE_INIT, reinterpret_cast<void*>(JniServiceInit));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_SERVICE_EXECUTE, reinterpret_cast<void*>(JniServiceExecute));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_SERVICE_CANCEL, reinterpret_cast<void*>(JniServiceCancel));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_SERVICE_INVOKE_METHOD, reinterpret_cast<void*>(JniServiceInvokeMethod));
					
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CLUSTER_NODE_FILTER_APPLY, reinterpret_cast<void*>(JniClusterNodeFilterApply));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_NODE_INFO, reinterpret_cast<void*>(JniNodeInfo));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_MEMORY_REALLOCATE, reinterpret_cast<void*>(JniMemoryReallocate));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_ON_START, reinterpret_cast<void*>(JniOnStart));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_ON_STOP, reinterpret_cast<void*>(JniOnStop));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_EXTENSION_CALLBACK_IN_LONG_OUT_LONG, reinterpret_cast<void*>(JniExtensionCallbackInLongOutLong));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_EXTENSION_CALLBACK_IN_LONG_LONG_OUT_LONG, reinterpret_cast<void*>(JniExtensionCallbackInLongLongOutLong));

                    jint res = env->RegisterNatives(FindClass(env, C_PLATFORM_CALLBACK_UTILS), methods, idx);

                    if (res != JNI_OK)
                        throw JvmException();
                }  
            }

            JniContext::JniContext(JniJvm* jvm, JniHandlers hnds) : jvm(jvm), hnds(hnds) {
                // No-op.
            }

            JniContext* JniContext::Create(char** opts, int optsLen, JniHandlers hnds) {
                return Create(opts, optsLen, hnds, NULL);
            }

            JniContext* JniContext::Create(char** opts, int optsLen, JniHandlers hnds, JniErrorInfo* errInfo)
            {
                // Acquire global lock to instantiate the JVM.
                JVM_LOCK.Enter();

                // Define local variables.
                JavaVM* jvm = NULL;
                JNIEnv* env = NULL;

                JniJavaMembers javaMembers;
                memset(&javaMembers, 0, sizeof(javaMembers));

                JniMembers members;
                memset(&members, 0, sizeof(members));

                JniContext* ctx = NULL;

                std::string errClsName;
                int errClsNameLen = 0;
                std::string errMsg;
                int errMsgLen = 0;

                try {
                    if (!JVM.GetJvm()) {
                        // 1. Create JVM itself.    
                        CreateJvm(opts, optsLen, &jvm, &env);

                        // 2. Populate members;
                        javaMembers.Initialize(env);
                        members.Initialize(env);

                        // 3. Register native functions.
                        RegisterNatives(env);

                        // 4. Create JNI JVM.
                        JVM = JniJvm(jvm, javaMembers, members);

                        char* printStack = getenv("IGNITE_CPP_PRINT_STACK");
                        PRINT_EXCEPTION = printStack && strcmp("true", printStack) == 0;
                    }

                    ctx = new JniContext(&JVM, hnds);
                }
                catch (JvmException)
                {
                    char* errClsNameChars = NULL;
                    char* errMsgChars = NULL;

                    // Read error info if possible.
                    javaMembers.WriteErrorInfo(env, &errClsNameChars, &errClsNameLen, &errMsgChars, &errMsgLen);

                    if (errClsNameChars) {
                        errClsName = errClsNameChars;

                        delete[] errClsNameChars;
                    }

                    if (errMsgChars)
                    {
                        errMsg = errMsgChars;

                        delete[] errMsgChars;
                    }

                    // Destroy mmebers.
                    if (env) {
                        members.Destroy(env);
                        javaMembers.Destroy(env);
                    }

                    // Destroy faulty JVM.
                    if (jvm)
                        jvm->DestroyJavaVM();
                }

                // It safe to release the lock at this point.
                JVM_LOCK.Leave();

                // Notify err callback if needed.
                if (!ctx) {
                    if (errInfo) {
                        JniErrorInfo errInfo0(IGNITE_JNI_ERR_JVM_INIT, errClsName.c_str(), errMsg.c_str());

                        *errInfo = errInfo0;
                    }

                    if (hnds.error)
                        hnds.error(hnds.target, IGNITE_JNI_ERR_JVM_INIT, errClsName.c_str(), errClsNameLen,
                            errMsg.c_str(), errMsgLen, NULL, 0);
                }

                return ctx;
            }

            int JniContext::Reallocate(long long memPtr, int cap) {
                JavaVM* jvm = JVM.GetJvm();

                JNIEnv* env;

                int attachRes = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);

                if (attachRes == JNI_OK)
                    AttachHelper::OnThreadAttach();
                else
                    return -1;

                env->CallStaticVoidMethod(JVM.GetMembers().c_PlatformUtils, JVM.GetMembers().m_PlatformUtils_reallocate, memPtr, cap);

                if (env->ExceptionCheck()) {
                    env->ExceptionClear();

                    return -1;
                }

                return 0;
            }

            void JniContext::Detach() {
                gcc::Memory::Fence();

                if (JVM.GetJvm()) {
                    JNIEnv* env;

                    JVM.GetJvm()->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);

                    if (env)
                        JVM.GetJvm()->DetachCurrentThread();
                }
            }

            jobject JniContext::IgnitionStart(char* cfgPath, char* name, int factoryId, long long dataPtr) {
                return IgnitionStart(cfgPath, name, factoryId, dataPtr, NULL);
            }
            
            jobject JniContext::IgnitionStart(char* cfgPath, char* name, int factoryId, long long dataPtr, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jstring cfgPath0 = env->NewStringUTF(cfgPath);
                jstring name0 = env->NewStringUTF(name);

                jobject interop = env->CallStaticObjectMethod(
                    jvm->GetMembers().c_PlatformIgnition,
                    jvm->GetMembers().m_PlatformIgnition_start,
                    cfgPath0,
                    name0,
                    factoryId,
                    reinterpret_cast<long long>(&hnds),
                    dataPtr
                );

                ExceptionCheck(env, errInfo);

                return LocalToGlobal(env, interop);
            }


            jobject JniContext::IgnitionInstance(char* name)
            {
                return IgnitionInstance(name, NULL);
            }

            jobject JniContext::IgnitionInstance(char* name, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jstring name0 = env->NewStringUTF(name);

                jobject interop = env->CallStaticObjectMethod(jvm->GetMembers().c_PlatformIgnition,
                    jvm->GetMembers().m_PlatformIgnition_instance, name0);

                ExceptionCheck(env, errInfo);

                return LocalToGlobal(env, interop);
            }

            long long JniContext::IgnitionEnvironmentPointer(char* name)
            {
                return IgnitionEnvironmentPointer(name, NULL);
            }

            long long JniContext::IgnitionEnvironmentPointer(char* name, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jstring name0 = env->NewStringUTF(name);

                long long res = env->CallStaticLongMethod(jvm->GetMembers().c_PlatformIgnition,
                    jvm->GetMembers().m_PlatformIgnition_environmentPointer, name0);

                ExceptionCheck(env, errInfo);

                return res;
            }

            bool JniContext::IgnitionStop(char* name, bool cancel)
            {
                return IgnitionStop(name, cancel, NULL);
            }

            bool JniContext::IgnitionStop(char* name, bool cancel, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jstring name0 = env->NewStringUTF(name);

                jboolean res = env->CallStaticBooleanMethod(jvm->GetMembers().c_PlatformIgnition,
                    jvm->GetMembers().m_PlatformIgnition_stop, name0, cancel);

                ExceptionCheck(env, errInfo);

                return res != 0;
            }

            void JniContext::IgnitionStopAll(bool cancel)
            {
                return IgnitionStopAll(cancel, NULL);
            }

            void JniContext::IgnitionStopAll(bool cancel, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                env->CallStaticVoidMethod(jvm->GetMembers().c_PlatformIgnition,
                    jvm->GetMembers().m_PlatformIgnition_stopAll, cancel);

                ExceptionCheck(env, errInfo);
            }

            void JniContext::ProcessorReleaseStart(jobject obj) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformProcessor_releaseStart);

                ExceptionCheck(env);
            }

            jobject JniContext::ProcessorProjection(jobject obj) {
                JNIEnv* env = Attach();

                jobject prj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_projection);

                ExceptionCheck(env);

                return LocalToGlobal(env, prj);
            }

            jobject JniContext::ProcessorCache0(jobject obj, const char* name, jmethodID mthd, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject cache = env->CallObjectMethod(obj, mthd, name0);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env, errInfo);

                return LocalToGlobal(env, cache);
            }

            jobject JniContext::ProcessorCache(jobject obj, const char* name) {
                return ProcessorCache(obj, name, NULL);
            }

            jobject JniContext::ProcessorCache(jobject obj, const char* name, JniErrorInfo* errInfo) {
                return ProcessorCache0(obj, name, jvm->GetMembers().m_PlatformProcessor_cache, errInfo);
            }

            jobject JniContext::ProcessorCreateCache(jobject obj, const char* name) {
                return ProcessorCreateCache(obj, name, NULL);
            }

            jobject JniContext::ProcessorCreateCache(jobject obj, const char* name, JniErrorInfo* errInfo)
            {
                return ProcessorCache0(obj, name, jvm->GetMembers().m_PlatformProcessor_createCache, errInfo);
            }

            jobject JniContext::ProcessorGetOrCreateCache(jobject obj, const char* name) {
                return ProcessorGetOrCreateCache(obj, name, NULL);
            }

            jobject JniContext::ProcessorGetOrCreateCache(jobject obj, const char* name, JniErrorInfo* errInfo)
            {
                return ProcessorCache0(obj, name, jvm->GetMembers().m_PlatformProcessor_getOrCreateCache, errInfo);
            }

            jobject JniContext::ProcessorAffinity(jobject obj, const char* name) {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject aff = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_affinity, name0);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env);

                return LocalToGlobal(env, aff);
            }

            jobject JniContext::ProcessorDataStreamer(jobject obj, const char* name, bool keepPortable) {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject ldr = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_dataStreamer, name0,
                    keepPortable);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env);

                return LocalToGlobal(env, ldr);
            }

            jobject JniContext::ProcessorTransactions(jobject obj) {
                JNIEnv* env = Attach();

                jobject tx = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_transactions);

                ExceptionCheck(env);

                return LocalToGlobal(env, tx);
            }
            
            jobject JniContext::ProcessorCompute(jobject obj, jobject prj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_compute, prj);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::ProcessorMessage(jobject obj, jobject prj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_message, prj);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::ProcessorEvents(jobject obj, jobject prj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_events, prj);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::ProcessorServices(jobject obj, jobject prj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_services, prj);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }
            
            jobject JniContext::ProcessorExtensions(jobject obj)
            {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_extensions);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            long long JniContext::TargetInStreamOutLong(jobject obj, int opType, long long memPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                long long res = env->CallLongMethod(obj, jvm->GetMembers().m_PlatformTarget_inStreamOutLong, opType, memPtr);

                ExceptionCheck(env, err);

                return res;
            }

            void JniContext::TargetInStreamOutStream(jobject obj, int opType, long long inMemPtr, long long outMemPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTarget_inStreamOutStream, opType, inMemPtr, outMemPtr);

                ExceptionCheck(env, err);
            }

           jobject JniContext::TargetInStreamOutObject(jobject obj, int opType, long long memPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformTarget_inStreamOutObject, opType, memPtr);

                ExceptionCheck(env, err);

                return LocalToGlobal(env, res);
            }

            void JniContext::TargetInObjectStreamOutStream(jobject obj, int opType, void* arg, long long inMemPtr, long long outMemPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTarget_inObjectStreamOutStream, opType, arg, inMemPtr, outMemPtr);

                ExceptionCheck(env, err);
            }

            long long JniContext::TargetOutLong(jobject obj, int opType, JniErrorInfo* err)
            {
                JNIEnv* env = Attach();

                jlong res = env->CallLongMethod(obj, jvm->GetMembers().m_PlatformTarget_outLong, opType);

                ExceptionCheck(env, err);

                return res;
            }

            void JniContext::TargetOutStream(jobject obj, int opType, long long memPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTarget_outStream, opType, memPtr);

                ExceptionCheck(env, err);
            }

            jobject JniContext::TargetOutObject(jobject obj, int opType, JniErrorInfo* err)
            {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformTarget_outObject, opType);

                ExceptionCheck(env, err);

                return LocalToGlobal(env, res);
            }

            void JniContext::TargetListenFuture(jobject obj, long long futId, int typ) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTarget_listenFuture, futId, typ);

                ExceptionCheck(env);
            }

            void JniContext::TargetListenFutureForOperation(jobject obj, long long futId, int typ, int opId) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTarget_listenFutureForOperation, futId, typ, opId);

                ExceptionCheck(env);
            }

            int JniContext::AffinityPartitions(jobject obj) {
                JNIEnv* env = Attach();

                jint parts = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformAffinity_partitions);

                ExceptionCheck(env);

                return parts;
            }

            jobject JniContext::CacheWithSkipStore(jobject obj) {
                JNIEnv* env = Attach();

                jobject cache = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_withSkipStore);

                ExceptionCheck(env);

                return LocalToGlobal(env, cache);
            }

            jobject JniContext::CacheWithNoRetries(jobject obj) {
                JNIEnv* env = Attach();

                jobject cache = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_withNoRetries);

                ExceptionCheck(env);

                return LocalToGlobal(env, cache);
            }

            jobject JniContext::CacheWithExpiryPolicy(jobject obj, long long create, long long update, long long access) {
                JNIEnv* env = Attach();

                jobject cache = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_withExpiryPolicy,
                    create, update, access);

                ExceptionCheck(env);

                return LocalToGlobal(env, cache);
            }

            jobject JniContext::CacheWithAsync(jobject obj) {
                JNIEnv* env = Attach();

                jobject cache = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_withAsync);

                ExceptionCheck(env);

                return LocalToGlobal(env, cache);
            }

            jobject JniContext::CacheWithKeepPortable(jobject obj) {
                JNIEnv* env = Attach();

                jobject cache = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_withKeepPortable);

                ExceptionCheck(env);

                return LocalToGlobal(env, cache);
            }

            void JniContext::CacheClear(jobject obj, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCache_clear);

                ExceptionCheck(env, err);
            }

            void JniContext::CacheRemoveAll(jobject obj, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCache_removeAll);

                ExceptionCheck(env, err);
            }

            jobject JniContext::CacheOutOpQueryCursor(jobject obj, int type, long long memPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(
                    obj, jvm->GetMembers().m_PlatformTarget_inStreamOutObject, type, memPtr);

                ExceptionCheck(env, err);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::CacheOutOpContinuousQuery(jobject obj, int type, long long memPtr) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(
                    obj, jvm->GetMembers().m_PlatformTarget_inStreamOutObject, type, memPtr);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::CacheIterator(jobject obj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_iterator);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::CacheLocalIterator(jobject obj, int peekModes) {
                JNIEnv*env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformCache_localIterator, peekModes);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            void JniContext::CacheEnterLock(jobject obj, long long id) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCache_enterLock, id);

                ExceptionCheck(env);
            }

            void JniContext::CacheExitLock(jobject obj, long long id) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCache_exitLock, id);

                ExceptionCheck(env);
            }

            bool JniContext::CacheTryEnterLock(jobject obj, long long id, long long timeout) {
                JNIEnv* env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformCache_tryEnterLock, id, timeout);

                ExceptionCheck(env);

                return res != 0;
            }

            void JniContext::CacheCloseLock(jobject obj, long long id) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCache_closeLock, id);

                ExceptionCheck(env);
            }

            void JniContext::CacheRebalance(jobject obj, long long futId) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCache_rebalance, futId);

                ExceptionCheck(env);
            }

            int JniContext::CacheSize(jobject obj, int peekModes, bool loc, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                jint res = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformCache_size, peekModes, loc);

                ExceptionCheck(env, err);

                return res;
            }

            void JniContext::CacheStoreCallbackInvoke(jobject obj, long long memPtr) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCacheStoreCallback_invoke, memPtr);

                ExceptionCheck(env);
            }

            void JniContext::ComputeWithNoFailover(jobject obj) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCompute_withNoFailover);

                ExceptionCheck(env);
            }

            void JniContext::ComputeWithTimeout(jobject obj, long long timeout) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCompute_withTimeout, timeout);

                ExceptionCheck(env);
            }

            void JniContext::ComputeExecuteNative(jobject obj, long long taskPtr, long long topVer) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformCompute_executeNative, taskPtr, topVer);

                ExceptionCheck(env);
            }

            void JniContext::ContinuousQueryClose(jobject obj) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformContinuousQuery_close);

                ExceptionCheck(env);
            }

            void* JniContext::ContinuousQueryGetInitialQueryCursor(jobject obj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformContinuousQuery_getInitialQueryCursor);

                ExceptionCheck(env);

                return res;
            }

            void JniContext::DataStreamerListenTopology(jobject obj, long long ptr) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_listenTopology, ptr);

                ExceptionCheck(env);
            }

            bool JniContext::DataStreamerAllowOverwriteGet(jobject obj) {
                JNIEnv* env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_getAllowOverwrite);

                ExceptionCheck(env);

                return res != 0;
            }

            void JniContext::DataStreamerAllowOverwriteSet(jobject obj, bool val) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_setAllowOverwrite, val);

                ExceptionCheck(env);
            }

            bool JniContext::DataStreamerSkipStoreGet(jobject obj) {
                JNIEnv* env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_getSkipStore);

                ExceptionCheck(env);

                return res != 0;
            }

            void JniContext::DataStreamerSkipStoreSet(jobject obj, bool val) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_setSkipStore, val);

                ExceptionCheck(env);
            }

            int JniContext::DataStreamerPerNodeBufferSizeGet(jobject obj) {
                JNIEnv* env = Attach();

                jint res = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_getPerNodeBufSize);

                ExceptionCheck(env);

                return res;
            }

            void JniContext::DataStreamerPerNodeBufferSizeSet(jobject obj, int val) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_setPerNodeBufSize, val);

                ExceptionCheck(env);
            }

            int JniContext::DataStreamerPerNodeParallelOperationsGet(jobject obj) {
                JNIEnv* env = Attach();

                jint res = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_getPerNodeParallelOps);

                ExceptionCheck(env);

                return res;
            }

            void JniContext::DataStreamerPerNodeParallelOperationsSet(jobject obj, int val) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformDataStreamer_setPerNodeParallelOps, val);

                ExceptionCheck(env);
            }

            jobject JniContext::MessagingWithAsync(jobject obj) {
                JNIEnv* env = Attach();

                jobject msg = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformMessaging_withAsync);

                ExceptionCheck(env);

                return LocalToGlobal(env, msg);
            }

            jobject JniContext::ProjectionForOthers(jobject obj, jobject prj) {
                JNIEnv* env = Attach();

                jobject newPrj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_forOthers, prj);

                ExceptionCheck(env);

                return LocalToGlobal(env, newPrj);
            }

            jobject JniContext::ProjectionForRemotes(jobject obj) {
                JNIEnv* env = Attach();

                jobject newPrj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_forRemotes);

                ExceptionCheck(env);

                return LocalToGlobal(env, newPrj);
            }

            jobject JniContext::ProjectionForDaemons(jobject obj) {
                JNIEnv* env = Attach();

                jobject newPrj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_forDaemons);

                ExceptionCheck(env);

                return LocalToGlobal(env, newPrj);
            }

            jobject JniContext::ProjectionForRandom(jobject obj) {
                JNIEnv* env = Attach();

                jobject newPrj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_forRandom);

                ExceptionCheck(env);

                return LocalToGlobal(env, newPrj);
            }

            jobject JniContext::ProjectionForOldest(jobject obj) {
                JNIEnv* env = Attach();

                jobject newPrj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_forOldest);

                ExceptionCheck(env);

                return LocalToGlobal(env, newPrj);
            }

            jobject JniContext::ProjectionForYoungest(jobject obj) {
                JNIEnv* env = Attach();

                jobject newPrj = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_forYoungest);

                ExceptionCheck(env);

                return LocalToGlobal(env, newPrj);
            }

            void JniContext::ProjectionResetMetrics(jobject obj) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformClusterGroup_resetMetrics);

                ExceptionCheck(env);
            }

            jobject JniContext::ProjectionOutOpRet(jobject obj, int type, long long memPtr) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(
                    obj, jvm->GetMembers().m_PlatformTarget_inStreamOutObject, type, memPtr);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }


            void JniContext::QueryCursorIterator(jobject obj, JniErrorInfo* errInfo) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformAbstractQryCursor_iter);

                ExceptionCheck(env, errInfo);
            }

            bool JniContext::QueryCursorIteratorHasNext(jobject obj, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformAbstractQryCursor_iterHasNext);

                ExceptionCheck(env, errInfo);

                return res != 0;
            }

            void JniContext::QueryCursorClose(jobject obj, JniErrorInfo* errInfo) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformAbstractQryCursor_close);

                ExceptionCheck(env, errInfo);
            }

            long long JniContext::TransactionsStart(jobject obj, int concurrency, int isolation, long long timeout, int txSize) {
                JNIEnv* env = Attach();

                long long id = env->CallLongMethod(obj, jvm->GetMembers().m_PlatformTransactions_txStart, concurrency, isolation, timeout, txSize);

                ExceptionCheck(env);

                return id;
            }

            int JniContext::TransactionsCommit(jobject obj, long long id) {
                JNIEnv* env = Attach();

                int res = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformTransactions_txCommit, id);

                ExceptionCheck(env);

                return res;
            }

            void JniContext::TransactionsCommitAsync(jobject obj, long long id, long long futId) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTransactions_txCommitAsync, id, futId);

                ExceptionCheck(env);
            }

            int JniContext::TransactionsRollback(jobject obj, long long id) {
                JNIEnv* env = Attach();

                int res = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformTransactions_txRollback, id);

                ExceptionCheck(env);

                return res;
            }

            void JniContext::TransactionsRollbackAsync(jobject obj, long long id, long long futId) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTransactions_txRollbackAsync, id, futId);

                ExceptionCheck(env);
            }

            int JniContext::TransactionsClose(jobject obj, long long id) {
                JNIEnv* env = Attach();

                jint state = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformTransactions_txClose, id);

                ExceptionCheck(env);

                return state;
            }

            int JniContext::TransactionsState(jobject obj, long long id) {
                JNIEnv* env = Attach();

                jint state = env->CallIntMethod(obj, jvm->GetMembers().m_PlatformTransactions_txState, id);

                ExceptionCheck(env);

                return state;
            }

            bool JniContext::TransactionsSetRollbackOnly(jobject obj, long long id) {
                JNIEnv* env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformTransactions_txSetRollbackOnly, id);

                ExceptionCheck(env);

                return res != 0;
            }

            void JniContext::TransactionsResetMetrics(jobject obj) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformTransactions_resetMetrics);

                ExceptionCheck(env);
            }

            jobject JniContext::EventsWithAsync(jobject obj) {
                JNIEnv * env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformEvents_withAsync);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            bool JniContext::EventsStopLocalListen(jobject obj, long long hnd) {
                JNIEnv * env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformEvents_stopLocalListen, hnd);

                ExceptionCheck(env);

                return res != 0;
            }

            void JniContext::EventsLocalListen(jobject obj, long long hnd, int type) {
                JNIEnv * env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformEvents_localListen, hnd, type);

                ExceptionCheck(env);
            }

            bool JniContext::EventsIsEnabled(jobject obj, int type) {
                JNIEnv * env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformEvents_isEnabled, type);

                ExceptionCheck(env);

                return res != 0;
            }
            
			jobject JniContext::ServicesWithAsync(jobject obj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformServices_withAsync);

				ExceptionCheck(env);

				return LocalToGlobal(env, res);
            }

            jobject JniContext::ServicesWithServerKeepPortable(jobject obj) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformServices_withServerKeepPortable);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

			void JniContext::ServicesCancel(jobject obj, char* name) {
                JNIEnv* env = Attach();

				jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformServices_cancel, name0);

				if (name0)
					env->DeleteLocalRef(name0);

				ExceptionCheck(env);
            }

			void JniContext::ServicesCancelAll(jobject obj) {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformServices_cancelAll);

				ExceptionCheck(env);
            }

			void* JniContext::ServicesGetServiceProxy(jobject obj, char* name, bool sticky) {
				JNIEnv* env = Attach();

				jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformServices_serviceProxy, name0, sticky);

				if (name0)
					env->DeleteLocalRef(name0);

				ExceptionCheck(env);

				return res;
			}

			jobject JniContext::Acquire(jobject obj)
            {
                if (obj) {

                    JNIEnv* env = Attach();

                    jobject obj0 = env->NewGlobalRef(obj);

                    ExceptionCheck(env);

                    return obj0;
                }
                
                return NULL;
            }

            void JniContext::Release(jobject obj) {
                if (obj)
                {
                    JavaVM* jvm = JVM.GetJvm();
                    
                    if (jvm)
                    {
                        JNIEnv* env;

                        jint attachRes = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);

                        if (attachRes == JNI_OK) 
                        {
                            AttachHelper::OnThreadAttach();

                            env->DeleteGlobalRef(obj);
                        }
                    }
                }
            }

            void JniContext::ThrowToJava(char* msg) {
                JNIEnv* env = Attach();

                env->ThrowNew(jvm->GetMembers().c_IgniteException, msg);
            }

            void JniContext::DestroyJvm() {
                jvm->GetJvm()->DestroyJavaVM();
            }

            /*
            * Attach thread to JVM.
            */
            JNIEnv* JniContext::Attach() {
                JNIEnv* env;

                jint attachRes = jvm->GetJvm()->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);

                if (attachRes == JNI_OK)
                    AttachHelper::OnThreadAttach();
                else {
                    if (hnds.error)
                        hnds.error(hnds.target, IGNITE_JNI_ERR_JVM_ATTACH, NULL, 0, NULL, 0, NULL, 0);
                }

                return env;
            }

            void JniContext::ExceptionCheck(JNIEnv* env) {
                ExceptionCheck(env, NULL);
            }

            void JniContext::ExceptionCheck(JNIEnv* env, JniErrorInfo* errInfo)
            {
                if (env->ExceptionCheck()) {
                    jthrowable err = env->ExceptionOccurred();

                    if (PRINT_EXCEPTION)
                        env->CallVoidMethod(err, jvm->GetJavaMembers().m_Throwable_printStackTrace);

                    env->ExceptionClear();

                    // Get error class name and message.
                    jclass cls = env->GetObjectClass(err);

                    jstring clsName = static_cast<jstring>(env->CallObjectMethod(cls, jvm->GetJavaMembers().m_Class_getName));
                    jstring msg = static_cast<jstring>(env->CallObjectMethod(err, jvm->GetJavaMembers().m_Throwable_getMessage));

                    env->DeleteLocalRef(cls);

                    int clsNameLen;
                    std::string clsName0 = JavaStringToCString(env, clsName, &clsNameLen);

                    int msgLen;
                    std::string msg0 = JavaStringToCString(env, msg, &msgLen);

                    if (errInfo)
                    {
                        JniErrorInfo errInfo0(IGNITE_JNI_ERR_GENERIC, clsName0.c_str(), msg0.c_str());

                        *errInfo = errInfo0;
                    }

                    // Get error additional data (if any).
                    jbyteArray errData = static_cast<jbyteArray>(env->CallStaticObjectMethod(
                        jvm->GetMembers().c_PlatformUtils, jvm->GetMembers().m_PlatformUtils_errData, err));

                    if (errData)
                    {
                        jbyte* errBytesNative = env->GetByteArrayElements(errData, NULL);

                        int errBytesLen = env->GetArrayLength(errData);

                        if (hnds.error)
                            hnds.error(hnds.target, IGNITE_JNI_ERR_GENERIC, clsName0.c_str(), clsNameLen, msg0.c_str(), msgLen,
                                errBytesNative, errBytesLen);
                        
                        env->ReleaseByteArrayElements(errData, errBytesNative, JNI_ABORT);
                    }
                    else
                    {
                        if (hnds.error)
                            hnds.error(hnds.target, IGNITE_JNI_ERR_GENERIC, clsName0.c_str(), clsNameLen, msg0.c_str(), msgLen,
                                NULL, 0);
                    }
                    
                    env->DeleteLocalRef(err);
                }
            }

            /*
            * Convert local reference to global.
            */
            jobject JniContext::LocalToGlobal(JNIEnv* env, jobject localRef) {
                if (localRef) {
                    jobject globalRef = env->NewGlobalRef(localRef);

                    env->DeleteLocalRef(localRef); // Clear local ref irrespective of result.

                    if (!globalRef)
                        ExceptionCheck(env);

                    return globalRef;
                }
                else
                    return NULL;
            }

            JNIEXPORT jlong JNICALL JniCacheStoreCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, CacheStoreCreateHandler, cacheStoreCreate, memPtr);
            }

            JNIEXPORT jint JNICALL JniCacheStoreInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr, jlong memPtr, jobject cb) {
                IGNITE_SAFE_FUNC(env, envPtr, CacheStoreInvokeHandler, cacheStoreInvoke, objPtr, memPtr, cb);
            }

            JNIEXPORT void JNICALL JniCacheStoreDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr) {
                IGNITE_SAFE_PROC(env, envPtr, CacheStoreDestroyHandler, cacheStoreDestroy, objPtr);
            }

            JNIEXPORT jlong JNICALL JniCacheStoreSessionCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong storePtr) {
                IGNITE_SAFE_FUNC(env, envPtr, CacheStoreSessionCreateHandler, cacheStoreSessionCreate, storePtr);
            }

            JNIEXPORT jlong JNICALL JniCacheEntryFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, CacheEntryFilterCreateHandler, cacheEntryFilterCreate, memPtr);
            }

            JNIEXPORT jint JNICALL JniCacheEntryFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, CacheEntryFilterApplyHandler, cacheEntryFilterApply, objPtr, memPtr);
            }

            JNIEXPORT void JNICALL JniCacheEntryFilterDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr) {
                IGNITE_SAFE_PROC(env, envPtr, CacheEntryFilterDestroyHandler, cacheEntryFilterDestroy, objPtr);
            }

            JNIEXPORT void JNICALL JniCacheInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong inMemPtr, jlong outMemPtr) {
                IGNITE_SAFE_PROC(env, envPtr, CacheInvokeHandler, cacheInvoke, inMemPtr, outMemPtr);
            }

            JNIEXPORT void JNICALL JniComputeTaskMap(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr, jlong inMemPtr, jlong outMemPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ComputeTaskMapHandler, computeTaskMap, taskPtr, inMemPtr, outMemPtr);
            }

            JNIEXPORT jint JNICALL JniComputeTaskJobResult(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr, jlong jobPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, ComputeTaskJobResultHandler, computeTaskJobRes, taskPtr, jobPtr, memPtr);
            }

            JNIEXPORT void JNICALL JniComputeTaskReduce(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ComputeTaskReduceHandler, computeTaskReduce, taskPtr);
            }

            JNIEXPORT void JNICALL JniComputeTaskComplete(JNIEnv *env, jclass cls, jlong envPtr, jlong taskPtr, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ComputeTaskCompleteHandler, computeTaskComplete, taskPtr, memPtr);
            }

            JNIEXPORT jint JNICALL JniComputeJobSerialize(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, ComputeJobSerializeHandler, computeJobSerialize, jobPtr, memPtr);
            }

            JNIEXPORT jlong JNICALL JniComputeJobCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, ComputeJobCreateHandler, computeJobCreate, memPtr);
            }

            JNIEXPORT void JNICALL JniComputeJobExecute(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr, jint cancel, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ComputeJobExecuteHandler, computeJobExec, jobPtr, cancel, memPtr);
            }

            JNIEXPORT void JNICALL JniComputeJobCancel(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ComputeJobCancelHandler, computeJobCancel, jobPtr);
            }

            JNIEXPORT void JNICALL JniComputeJobDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong jobPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ComputeJobDestroyHandler, computeJobDestroy, jobPtr);
            }
            
            JNIEXPORT void JNICALL JniContinuousQueryListenerApply(JNIEnv *env, jclass cls, jlong envPtr, jlong cbPtr, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ContinuousQueryListenerApplyHandler, contQryLsnrApply, cbPtr, memPtr);
            }

            JNIEXPORT jlong JNICALL JniContinuousQueryFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, ContinuousQueryFilterCreateHandler, contQryFilterCreate, memPtr);
            }

            JNIEXPORT jint JNICALL JniContinuousQueryFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong filterPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, ContinuousQueryFilterApplyHandler, contQryFilterApply, filterPtr, memPtr);
            }

            JNIEXPORT void JNICALL JniContinuousQueryFilterRelease(JNIEnv *env, jclass cls, jlong envPtr, jlong filterPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ContinuousQueryFilterReleaseHandler, contQryFilterRelease, filterPtr);
            }

            JNIEXPORT void JNICALL JniDataStreamerTopologyUpdate(JNIEnv *env, jclass cls, jlong envPtr, jlong ldrPtr, jlong topVer, jint topSize) {
                IGNITE_SAFE_PROC(env, envPtr, DataStreamerTopologyUpdateHandler, dataStreamerTopologyUpdate, ldrPtr, topVer, topSize);
            }

            JNIEXPORT void JNICALL JniDataStreamerStreamReceiverInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jobject cache, jlong memPtr, jboolean keepPortable) {
                IGNITE_SAFE_PROC(env, envPtr, DataStreamerStreamReceiverInvokeHandler, streamReceiverInvoke, ptr, cache, memPtr, keepPortable);
            }

            JNIEXPORT void JNICALL JniFutureByteResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureByteResultHandler, futByteRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureBoolResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureBoolResultHandler, futBoolRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureShortResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureShortResultHandler, futShortRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureCharResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureCharResultHandler, futCharRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureIntResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jint res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureIntResultHandler, futIntRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureFloatResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jfloat res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureFloatResultHandler, futFloatRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureLongResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jlong res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureLongResultHandler, futLongRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureDoubleResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jdouble res) {
                IGNITE_SAFE_PROC(env, envPtr, FutureDoubleResultHandler, futDoubleRes, futPtr, res);
            }

            JNIEXPORT void JNICALL JniFutureObjectResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, FutureObjectResultHandler, futObjRes, futPtr, memPtr);
            }

            JNIEXPORT void JNICALL JniFutureNullResult(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr) {
                IGNITE_SAFE_PROC(env, envPtr, FutureNullResultHandler, futNullRes, futPtr);
            }

            JNIEXPORT void JNICALL JniFutureError(JNIEnv *env, jclass cls, jlong envPtr, jlong futPtr, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, FutureErrorHandler, futErr, futPtr, memPtr);
            }

            JNIEXPORT void JNICALL JniLifecycleEvent(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jint evt) {
                IGNITE_SAFE_PROC(env, envPtr, LifecycleEventHandler, lifecycleEvt, ptr, evt);
            }

            JNIEXPORT void JNICALL JniMemoryReallocate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr, jint cap) {
                IGNITE_SAFE_PROC(env, envPtr, MemoryReallocateHandler, memRealloc, memPtr, cap);
            }

            JNIEXPORT jlong JNICALL JniMessagingFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, MessagingFilterCreateHandler, messagingFilterCreate, memPtr);
            }

            JNIEXPORT jint JNICALL JniMessagingFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, MessagingFilterApplyHandler, messagingFilterApply, ptr, memPtr);
            }

            JNIEXPORT void JNICALL JniMessagingFilterDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr) {
                IGNITE_SAFE_PROC(env, envPtr, MessagingFilterDestroyHandler, messagingFilterDestroy, ptr);
            }
            
            JNIEXPORT jlong JNICALL JniEventFilterCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, EventFilterCreateHandler, eventFilterCreate, memPtr);
            }

            JNIEXPORT jint JNICALL JniEventFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, EventFilterApplyHandler, eventFilterApply, ptr, memPtr);
            }

            JNIEXPORT void JNICALL JniEventFilterDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr) {
                IGNITE_SAFE_PROC(env, envPtr, EventFilterDestroyHandler, eventFilterDestroy, ptr);
            }

            JNIEXPORT jlong JNICALL JniServiceInit(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, ServiceInitHandler, serviceInit, memPtr);
            }

			JNIEXPORT void JNICALL JniServiceExecute(JNIEnv *env, jclass cls, jlong envPtr, jlong svcPtr, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ServiceExecuteHandler, serviceExecute, svcPtr, memPtr);
            }

			JNIEXPORT void JNICALL JniServiceCancel(JNIEnv *env, jclass cls, jlong envPtr, jlong svcPtr, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ServiceCancelHandler, serviceCancel, svcPtr, memPtr);
            }

			JNIEXPORT void JNICALL JniServiceInvokeMethod(JNIEnv *env, jclass cls, jlong envPtr, jlong svcPtr, jlong inMemPtr, jlong outMemPtr) {
                IGNITE_SAFE_PROC(env, envPtr, ServiceInvokeMethodHandler, serviceInvokeMethod, svcPtr, inMemPtr, outMemPtr);
            }

			JNIEXPORT jint JNICALL JniClusterNodeFilterApply(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
				IGNITE_SAFE_FUNC(env, envPtr, ClusterNodeFilterApplyHandler, clusterNodeFilterApply, memPtr);
            }

            JNIEXPORT jlong JNICALL JniNodeInfo(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr) {
                IGNITE_SAFE_FUNC(env, envPtr, NodeInfoHandler, nodeInfo, memPtr);
            }

            JNIEXPORT void JNICALL JniOnStart(JNIEnv *env, jclass cls, jlong envPtr, jobject proc, jlong memPtr) {
                IGNITE_SAFE_PROC(env, envPtr, OnStartHandler, onStart, env->NewGlobalRef(proc), memPtr);
            }

            JNIEXPORT void JNICALL JniOnStop(JNIEnv *env, jclass cls, jlong envPtr) {
                IGNITE_SAFE_PROC_NO_ARG(env, envPtr, OnStopHandler, onStop);
            }
            
            JNIEXPORT jlong JNICALL JniExtensionCallbackInLongOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint typ, jlong arg1) {
                IGNITE_SAFE_FUNC(env, envPtr, ExtensionCallbackInLongOutLongHandler, extensionCallbackInLongOutLong, typ, arg1);
            }

            JNIEXPORT jlong JNICALL JniExtensionCallbackInLongLongOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint typ, jlong arg1, jlong arg2) {
                IGNITE_SAFE_FUNC(env, envPtr, ExtensionCallbackInLongLongOutLongHandler, extensionCallbackInLongLongOutLong, typ, arg1, arg2);
            }
        }
    }
}