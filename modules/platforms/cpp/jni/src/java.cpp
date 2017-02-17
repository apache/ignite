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

// ReSharper disable once CppUnusedIncludeDirective
#include <cstring>   // needed only on linux
#include <string>
#include <exception>
#include <vector>
#include <algorithm>
#include <stdexcept>

#include "ignite/jni/utils.h"
#include "ignite/common/concurrent.h"
#include "ignite/jni/java.h"
#include <ignite/ignite_error.h>

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

using namespace ignite::java;

namespace ignite
{
    namespace jni
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

            /**
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

            /**
             * Guard to ensure global reference cleanup.
             */
            class JniGlobalRefGuard
            {
            public:
                JniGlobalRefGuard(JNIEnv *e, jobject obj) : env(e), ref(obj)
                {
                    // No-op.
                }

                ~JniGlobalRefGuard()
                {
                    env->DeleteGlobalRef(ref);
                }

            private:
                /** Environment. */
                JNIEnv* env;

                /** Target reference. */
                jobject ref;

                IGNITE_NO_COPY_ASSIGNMENT(JniGlobalRefGuard)
            };

            const char* C_THROWABLE = "java/lang/Throwable";
            JniMethod M_THROWABLE_GET_MESSAGE = JniMethod("getMessage", "()Ljava/lang/String;", false);
            JniMethod M_THROWABLE_PRINT_STACK_TRACE = JniMethod("printStackTrace", "()V", false);

            const char* C_CLASS = "java/lang/Class";
            JniMethod M_CLASS_GET_NAME = JniMethod("getName", "()Ljava/lang/String;", false);

            const char* C_IGNITE_EXCEPTION = "org/apache/ignite/IgniteException";

            const char* C_PLATFORM_NO_CALLBACK_EXCEPTION = "org/apache/ignite/internal/processors/platform/PlatformNoCallbackException";

            const char* C_PLATFORM_PROCESSOR = "org/apache/ignite/internal/processors/platform/PlatformProcessor";
            JniMethod M_PLATFORM_PROCESSOR_RELEASE_START = JniMethod("releaseStart", "()V", false);
            JniMethod M_PLATFORM_PROCESSOR_PROJECTION = JniMethod("projection", "()Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_CACHE = JniMethod("cache", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_CREATE_CACHE = JniMethod("createCache", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_GET_OR_CREATE_CACHE = JniMethod("getOrCreateCache", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_CREATE_CACHE_FROM_CONFIG = JniMethod("createCacheFromConfig", "(J)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_GET_OR_CREATE_CACHE_FROM_CONFIG = JniMethod("getOrCreateCacheFromConfig", "(J)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_CREATE_NEAR_CACHE = JniMethod("createNearCache", "(Ljava/lang/String;J)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_GET_OR_CREATE_NEAR_CACHE = JniMethod("getOrCreateNearCache", "(Ljava/lang/String;J)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_DESTROY_CACHE = JniMethod("destroyCache", "(Ljava/lang/String;)V", false);
            JniMethod M_PLATFORM_PROCESSOR_AFFINITY = JniMethod("affinity", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_DATA_STREAMER = JniMethod("dataStreamer", "(Ljava/lang/String;Z)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_TRANSACTIONS = JniMethod("transactions", "()Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_COMPUTE = JniMethod("compute", "(Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_MESSAGE = JniMethod("message", "(Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_EVENTS = JniMethod("events", "(Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_SERVICES = JniMethod("services", "(Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_EXTENSIONS = JniMethod("extensions", "()Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_ATOMIC_LONG = JniMethod("atomicLong", "(Ljava/lang/String;JZ)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_ATOMIC_SEQUENCE = JniMethod("atomicSequence", "(Ljava/lang/String;JZ)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_ATOMIC_REFERENCE = JniMethod("atomicReference", "(Ljava/lang/String;JZ)Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);
            JniMethod M_PLATFORM_PROCESSOR_GET_IGNITE_CONFIGURATION = JniMethod("getIgniteConfiguration", "(J)V", false);
            JniMethod M_PLATFORM_PROCESSOR_GET_CACHE_NAMES = JniMethod("getCacheNames", "(J)V", false);
            JniMethod M_PLATFORM_PROCESSOR_LOGGER_IS_LEVEL_ENABLED = JniMethod("loggerIsLevelEnabled", "(I)Z", false);
            JniMethod M_PLATFORM_PROCESSOR_LOGGER_LOG = JniMethod("loggerLog", "(ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", false);
            JniMethod M_PLATFORM_PROCESSOR_BINARY_PROCESSOR = JniMethod("binaryProcessor", "()Lorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;", false);

            const char* C_PLATFORM_TARGET = "org/apache/ignite/internal/processors/platform/PlatformTargetProxy";
            JniMethod M_PLATFORM_TARGET_IN_LONG_OUT_LONG = JniMethod("inLongOutLong", "(IJ)J", false);
            JniMethod M_PLATFORM_TARGET_IN_STREAM_OUT_LONG = JniMethod("inStreamOutLong", "(IJ)J", false);
            JniMethod M_PLATFORM_TARGET_IN_STREAM_OUT_OBJECT = JniMethod("inStreamOutObject", "(IJ)Ljava/lang/Object;", false);
            JniMethod M_PLATFORM_TARGET_IN_STREAM_OUT_STREAM = JniMethod("inStreamOutStream", "(IJJ)V", false);
            JniMethod M_PLATFORM_TARGET_IN_OBJECT_STREAM_OUT_OBJECT_STREAM = JniMethod("inObjectStreamOutObjectStream", "(ILjava/lang/Object;JJ)Ljava/lang/Object;", false);
            JniMethod M_PLATFORM_TARGET_OUT_STREAM = JniMethod("outStream", "(IJ)V", false);
            JniMethod M_PLATFORM_TARGET_OUT_OBJECT = JniMethod("outObject", "(I)Ljava/lang/Object;", false);
            JniMethod M_PLATFORM_TARGET_LISTEN_FUTURE = JniMethod("listenFuture", "(JI)V", false);
            JniMethod M_PLATFORM_TARGET_LISTEN_FOR_OPERATION = JniMethod("listenFutureForOperation", "(JII)V", false);

            const char* C_PLATFORM_CALLBACK_UTILS = "org/apache/ignite/internal/processors/platform/callback/PlatformCallbackUtils";

            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_CREATE = JniMethod("cacheStoreCreate", "(JJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_CACHE_STORE_INVOKE = JniMethod("cacheStoreInvoke", "(JJJ)I", true);
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
            JniMethod M_PLATFORM_CALLBACK_UTILS_DATA_STREAMER_STREAM_RECEIVER_INVOKE = JniMethod("dataStreamerStreamReceiverInvoke", "(JJLorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;JZ)V", true);

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

            JniMethod M_PLATFORM_CALLBACK_UTILS_ON_CLIENT_DISCONNECTED = JniMethod("onClientDisconnected", "(J)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_ON_CLIENT_RECONNECTED = JniMethod("onClientReconnected", "(JZ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_LOGGER_LOG = JniMethod("loggerLog", "(JILjava/lang/String;Ljava/lang/String;Ljava/lang/String;J)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_LOGGER_IS_LEVEL_ENABLED = JniMethod("loggerIsLevelEnabled", "(JI)Z", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_AFFINITY_FUNCTION_INIT = JniMethod("affinityFunctionInit", "(JJLorg/apache/ignite/internal/processors/platform/PlatformTargetProxy;)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_AFFINITY_FUNCTION_PARTITION = JniMethod("affinityFunctionPartition", "(JJJ)I", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_AFFINITY_FUNCTION_ASSIGN_PARTITIONS = JniMethod("affinityFunctionAssignPartitions", "(JJJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_AFFINITY_FUNCTION_REMOVE_NODE = JniMethod("affinityFunctionRemoveNode", "(JJJ)V", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_AFFINITY_FUNCTION_DESTROY = JniMethod("affinityFunctionDestroy", "(JJ)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_CONSOLE_WRITE = JniMethod("consoleWrite", "(Ljava/lang/String;Z)V", true);

            JniMethod M_PLATFORM_CALLBACK_UTILS_IN_LONG_OUT_LONG = JniMethod("inLongOutLong", "(JIJ)J", true);
            JniMethod M_PLATFORM_CALLBACK_UTILS_IN_LONG_LONG_LONG_OBJECT_OUT_LONG = JniMethod("inLongLongLongObjectOutLong", "(JIJJJLjava/lang/Object;)J", true);

            const char* C_PLATFORM_UTILS = "org/apache/ignite/internal/processors/platform/utils/PlatformUtils";
            JniMethod M_PLATFORM_UTILS_REALLOC = JniMethod("reallocate", "(JI)V", true);
            JniMethod M_PLATFORM_UTILS_ERR_DATA = JniMethod("errorData", "(Ljava/lang/Throwable;)[B", true);
            JniMethod M_PLATFORM_UTILS_GET_FULL_STACK_TRACE = JniMethod("getFullStackTrace", "(Ljava/lang/Throwable;)Ljava/lang/String;", true);

            const char* C_PLATFORM_IGNITION = "org/apache/ignite/internal/processors/platform/PlatformIgnition";
            JniMethod M_PLATFORM_IGNITION_START = JniMethod("start", "(Ljava/lang/String;Ljava/lang/String;IJJ)Lorg/apache/ignite/internal/processors/platform/PlatformProcessor;", true);
            JniMethod M_PLATFORM_IGNITION_INSTANCE = JniMethod("instance", "(Ljava/lang/String;)Lorg/apache/ignite/internal/processors/platform/PlatformProcessor;", true);
            JniMethod M_PLATFORM_IGNITION_ENVIRONMENT_POINTER = JniMethod("environmentPointer", "(Ljava/lang/String;)J", true);
            JniMethod M_PLATFORM_IGNITION_STOP = JniMethod("stop", "(Ljava/lang/String;Z)Z", true);
            JniMethod M_PLATFORM_IGNITION_STOP_ALL = JniMethod("stopAll", "(Z)V", true);

            /* STATIC STATE. */
            gcc::CriticalSection JVM_LOCK;
            gcc::CriticalSection CONSOLE_LOCK;
            JniJvm JVM;
            bool PRINT_EXCEPTION = false;
            std::vector<ConsoleWriteHandler> consoleWriteHandlers;

            /* HELPER METHODS. */

            /**
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

                c_PlatformUtils = FindClass(env, C_PLATFORM_UTILS);
                m_PlatformUtils_getFullStackTrace = FindMethod(env, c_PlatformUtils, M_PLATFORM_UTILS_GET_FULL_STACK_TRACE);
            }

            void JniJavaMembers::Destroy(JNIEnv* env) {
                DeleteClass(env, c_Class);
                DeleteClass(env, c_Throwable);
                DeleteClass(env, c_PlatformUtils);
            }

            bool JniJavaMembers::WriteErrorInfo(JNIEnv* env, char** errClsName, int* errClsNameLen, char** errMsg,
				int* errMsgLen, char** stackTrace, int* stackTraceLen) {
                if (env && env->ExceptionCheck()) {
                    if (m_Class_getName && m_Throwable_getMessage) {
                        jthrowable err = env->ExceptionOccurred();

                        env->ExceptionClear();

                        jclass errCls = env->GetObjectClass(err);

                        jstring clsName = static_cast<jstring>(env->CallObjectMethod(errCls, m_Class_getName));
                        *errClsName = StringToChars(env, clsName, errClsNameLen);

                        jstring msg = static_cast<jstring>(env->CallObjectMethod(err, m_Throwable_getMessage));
                        *errMsg = StringToChars(env, msg, errMsgLen);

                        jstring trace = NULL;

                        if (c_PlatformUtils && m_PlatformUtils_getFullStackTrace) {
                            trace = static_cast<jstring>(env->CallStaticObjectMethod(c_PlatformUtils, m_PlatformUtils_getFullStackTrace, err));
                            *stackTrace = StringToChars(env, trace, stackTraceLen);
                        }

                        if (errCls)
                            env->DeleteLocalRef(errCls);

                        if (clsName)
                            env->DeleteLocalRef(clsName);

                        if (msg)
                            env->DeleteLocalRef(msg);

                        if (trace)
                            env->DeleteLocalRef(trace);

                        return true;
                    }
                    else {
                        env->ExceptionClear();
                    }
                }

                return false;
            }

            void JniMembers::Initialize(JNIEnv* env) {
                c_IgniteException = FindClass(env, C_IGNITE_EXCEPTION);

                c_PlatformIgnition = FindClass(env, C_PLATFORM_IGNITION);
                m_PlatformIgnition_start = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_START);
                m_PlatformIgnition_instance = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_INSTANCE);
                m_PlatformIgnition_environmentPointer = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_ENVIRONMENT_POINTER);
                m_PlatformIgnition_stop = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_STOP);
                m_PlatformIgnition_stopAll = FindMethod(env, c_PlatformIgnition, M_PLATFORM_IGNITION_STOP_ALL);

                c_PlatformProcessor = FindClass(env, C_PLATFORM_PROCESSOR);
                m_PlatformProcessor_releaseStart = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_RELEASE_START);
                m_PlatformProcessor_cache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_CACHE);
                m_PlatformProcessor_createCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_CREATE_CACHE);
                m_PlatformProcessor_getOrCreateCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_GET_OR_CREATE_CACHE);
                m_PlatformProcessor_createCacheFromConfig = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_CREATE_CACHE_FROM_CONFIG);
                m_PlatformProcessor_getOrCreateCacheFromConfig = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_GET_OR_CREATE_CACHE_FROM_CONFIG);
                m_PlatformProcessor_createNearCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_CREATE_NEAR_CACHE);
                m_PlatformProcessor_getOrCreateNearCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_GET_OR_CREATE_NEAR_CACHE);
                m_PlatformProcessor_destroyCache = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_DESTROY_CACHE);
                m_PlatformProcessor_affinity = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_AFFINITY);
                m_PlatformProcessor_dataStreamer = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_DATA_STREAMER);
                m_PlatformProcessor_transactions = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_TRANSACTIONS);
                m_PlatformProcessor_projection = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_PROJECTION);
                m_PlatformProcessor_compute = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_COMPUTE);
                m_PlatformProcessor_message = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_MESSAGE);
                m_PlatformProcessor_events = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_EVENTS);
                m_PlatformProcessor_services = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_SERVICES);
                m_PlatformProcessor_extensions = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_EXTENSIONS);
                m_PlatformProcessor_atomicLong = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_ATOMIC_LONG);
                m_PlatformProcessor_atomicSequence = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_ATOMIC_SEQUENCE);
                m_PlatformProcessor_atomicReference = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_ATOMIC_REFERENCE);
				m_PlatformProcessor_getIgniteConfiguration = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_GET_IGNITE_CONFIGURATION);
				m_PlatformProcessor_getCacheNames = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_GET_CACHE_NAMES);
				m_PlatformProcessor_loggerIsLevelEnabled = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_LOGGER_IS_LEVEL_ENABLED);
				m_PlatformProcessor_loggerLog = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_LOGGER_LOG);
				m_PlatformProcessor_binaryProcessor = FindMethod(env, c_PlatformProcessor, M_PLATFORM_PROCESSOR_BINARY_PROCESSOR);

                c_PlatformTarget = FindClass(env, C_PLATFORM_TARGET);
                m_PlatformTarget_inLongOutLong = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_LONG_OUT_LONG);
                m_PlatformTarget_inStreamOutLong = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_STREAM_OUT_LONG);
                m_PlatformTarget_inStreamOutObject = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_STREAM_OUT_OBJECT);
                m_PlatformTarget_outStream = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_OUT_STREAM);
                m_PlatformTarget_outObject = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_OUT_OBJECT);
                m_PlatformTarget_inStreamOutStream = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_STREAM_OUT_STREAM);
                m_PlatformTarget_inObjectStreamOutObjectStream = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_IN_OBJECT_STREAM_OUT_OBJECT_STREAM);
                m_PlatformTarget_listenFuture = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_LISTEN_FUTURE);
                m_PlatformTarget_listenFutureForOperation = FindMethod(env, c_PlatformTarget, M_PLATFORM_TARGET_LISTEN_FOR_OPERATION);

                c_PlatformUtils = FindClass(env, C_PLATFORM_UTILS);
                m_PlatformUtils_reallocate = FindMethod(env, c_PlatformUtils, M_PLATFORM_UTILS_REALLOC);
                m_PlatformUtils_errData = FindMethod(env, c_PlatformUtils, M_PLATFORM_UTILS_ERR_DATA);

                // Find utility classes which are not used from context, but are still required in other places.
                CheckClass(env, C_PLATFORM_NO_CALLBACK_EXCEPTION);
            }

            void JniMembers::Destroy(JNIEnv* env) {
                DeleteClass(env, c_IgniteException);
                DeleteClass(env, c_PlatformIgnition);
                DeleteClass(env, c_PlatformProcessor);
                DeleteClass(env, c_PlatformTarget);
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

            /**
             * Create JVM.
             */
            jint CreateJvm(char** opts, int optsLen, JavaVM** jvm, JNIEnv** env) {
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

                return res;
            }

            void RegisterNatives(JNIEnv* env) {
                {
					JNINativeMethod methods[5];

                    int idx = 0;

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_CONSOLE_WRITE, reinterpret_cast<void*>(JniConsoleWrite));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_LOGGER_LOG, reinterpret_cast<void*>(JniLoggerLog));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_LOGGER_IS_LEVEL_ENABLED, reinterpret_cast<void*>(JniLoggerIsLevelEnabled));

                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_IN_LONG_OUT_LONG, reinterpret_cast<void*>(JniInLongOutLong));
                    AddNativeMethod(methods + idx++, M_PLATFORM_CALLBACK_UTILS_IN_LONG_LONG_LONG_OBJECT_OUT_LONG, reinterpret_cast<void*>(JniInLongLongLongObjectOutLong));

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

            void GetJniErrorMessage(std::string& errMsg, jint res)
            {
                switch (res)
                {
                    case JNI_ERR:
                        errMsg = "Unknown error (JNI_ERR).";
                        break;

                    case JNI_EDETACHED:
                        errMsg = "Thread detached from the JVM.";
                        break;

                    case JNI_EVERSION:
                        errMsg = "JNI version error.";
                        break;

                    case JNI_ENOMEM:
                        errMsg = "Could not reserve enough space for object heap. Check Xmx option.";
                        break;

                    case JNI_EEXIST:
                        errMsg = "JVM already created.";
                        break;

                    case JNI_EINVAL:
                        errMsg = "Invalid JVM arguments.";
                        break;

                    default:
                        errMsg = "Unexpected JNI_CreateJavaVM result.";
                        break;
                }
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
                std::string stackTrace;
                int stackTraceLen = 0;

                try {
                    if (!JVM.GetJvm())
                    {
                        // 1. Create JVM itself.
                        jint res = CreateJvm(opts, optsLen, &jvm, &env);

                        if (res == JNI_OK)
                        {
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
                        else
                        {
                            GetJniErrorMessage(errMsg, res);

                            errMsgLen = static_cast<int>(errMsg.length());
                        }
                    }

                    if (JVM.GetJvm())
                        ctx = new JniContext(&JVM, hnds);
                }
                catch (JvmException)
                {
                    char* errClsNameChars = NULL;
                    char* errMsgChars = NULL;
                    char* stackTraceChars = NULL;

                    // Read error info if possible.
                    javaMembers.WriteErrorInfo(env, &errClsNameChars, &errClsNameLen, &errMsgChars, &errMsgLen,
						&stackTraceChars, &stackTraceLen);

                    if (errClsNameChars) {
                        errClsName = errClsNameChars;

                        delete[] errClsNameChars;
                    }

                    if (errMsgChars)
                    {
                        errMsg = errMsgChars;

                        delete[] errMsgChars;
                    }

                    if (stackTraceChars)
                    {
                        stackTrace = stackTraceChars;

                        delete[] stackTraceChars;
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
                            errMsg.c_str(), errMsgLen, stackTrace.c_str(), stackTraceLen, NULL, 0);
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

            jobject JniContext::ProcessorCacheFromConfig0(jobject obj, long long memPtr, jmethodID mthd, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jobject cache = env->CallObjectMethod(obj, mthd, memPtr);

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

            void JniContext::ProcessorDestroyCache(jobject obj, const char* name) {
                ProcessorDestroyCache(obj, name, NULL);
            }

            void JniContext::ProcessorDestroyCache(jobject obj, const char* name, JniErrorInfo* errInfo)
            {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformProcessor_destroyCache, name0);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env, errInfo);
            }

            jobject JniContext::ProcessorCreateCacheFromConfig(jobject obj, long long memPtr) {
                return ProcessorCreateCacheFromConfig(obj, memPtr, NULL);
            }

            jobject JniContext::ProcessorCreateCacheFromConfig(jobject obj, long long memPtr, JniErrorInfo* errInfo)
            {
                return ProcessorCacheFromConfig0(obj, memPtr, jvm->GetMembers().m_PlatformProcessor_createCacheFromConfig, errInfo);
            }

            jobject JniContext::ProcessorGetOrCreateCacheFromConfig(jobject obj, long long memPtr) {
                return ProcessorGetOrCreateCacheFromConfig(obj, memPtr, NULL);
            }

            jobject JniContext::ProcessorGetOrCreateCacheFromConfig(jobject obj, long long memPtr, JniErrorInfo* errInfo)
            {
                return ProcessorCacheFromConfig0(obj, memPtr, jvm->GetMembers().m_PlatformProcessor_getOrCreateCacheFromConfig, errInfo);
            }

            jobject JniContext::ProcessorCreateNearCache(jobject obj, const char* name, long long memPtr)
            {
                return ProcessorGetOrCreateNearCache0(obj, name, memPtr, jvm->GetMembers().m_PlatformProcessor_createNearCache);
            }

            jobject JniContext::ProcessorGetOrCreateNearCache(jobject obj, const char* name, long long memPtr)
            {
                return ProcessorGetOrCreateNearCache0(obj, name, memPtr, jvm->GetMembers().m_PlatformProcessor_getOrCreateNearCache);
            }

            jobject JniContext::ProcessorGetOrCreateNearCache0(jobject obj, const char* name, long long memPtr, jmethodID methodID)
            {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject cache = env->CallObjectMethod(obj, methodID, name0, memPtr);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env);

                return LocalToGlobal(env, cache);
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

            jobject JniContext::ProcessorTransactions(jobject obj, JniErrorInfo* errInfo) {
                JNIEnv* env = Attach();

                jobject tx = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_transactions);

                ExceptionCheck(env, errInfo);

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

            jobject JniContext::ProcessorAtomicLong(jobject obj, char* name, long long initVal, bool create)
            {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_atomicLong, name0, initVal, create);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::ProcessorAtomicSequence(jobject obj, char* name, long long initVal, bool create)
            {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_atomicSequence, name0, initVal, create);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::ProcessorAtomicReference(jobject obj, char* name, long long memPtr, bool create)
            {
                JNIEnv* env = Attach();

                jstring name0 = name != NULL ? env->NewStringUTF(name) : NULL;

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_atomicReference, name0, memPtr, create);

                if (name0)
                    env->DeleteLocalRef(name0);

                ExceptionCheck(env);

                return LocalToGlobal(env, res);
            }

            void JniContext::ProcessorGetIgniteConfiguration(jobject obj, long long memPtr)
            {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformProcessor_getIgniteConfiguration, memPtr);

                ExceptionCheck(env);
            }

            void JniContext::ProcessorGetCacheNames(jobject obj, long long memPtr)
            {
                JNIEnv* env = Attach();

                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformProcessor_getCacheNames, memPtr);

                ExceptionCheck(env);
            }

            long long JniContext::TargetInLongOutLong(jobject obj, int opType, long long val, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                long long res = env->CallLongMethod(obj, jvm->GetMembers().m_PlatformTarget_inLongOutLong, opType, val);

                ExceptionCheck(env, err);

                return res;
            }

            bool JniContext::ProcessorLoggerIsLevelEnabled(jobject obj, int level)
            {
                JNIEnv* env = Attach();

                jboolean res = env->CallBooleanMethod(obj, jvm->GetMembers().m_PlatformProcessor_loggerIsLevelEnabled, level);

                ExceptionCheck(env);

                return res != 0;
            }

            void JniContext::ProcessorLoggerLog(jobject obj, int level, char* message, char* category, char* errorInfo)
            {
                JNIEnv* env = Attach();

                jstring message0 = message != NULL ? env->NewStringUTF(message) : NULL;
                jstring category0 = category != NULL ? env->NewStringUTF(category) : NULL;
                jstring errorInfo0 = errorInfo != NULL ? env->NewStringUTF(errorInfo) : NULL;


                env->CallVoidMethod(obj, jvm->GetMembers().m_PlatformProcessor_loggerLog, level, message0, category0, errorInfo0);

                if (message0)
                    env->DeleteLocalRef(message0);

                if (category0)
                    env->DeleteLocalRef(category0);

                if (errorInfo0)
                    env->DeleteLocalRef(errorInfo0);

                ExceptionCheck(env);
            }

            jobject JniContext::ProcessorBinaryProcessor(jobject obj)
            {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformProcessor_binaryProcessor);

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

            jobject JniContext::TargetInObjectStreamOutObjectStream(jobject obj, int opType, void* arg, long long inMemPtr, long long outMemPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(obj, jvm->GetMembers().m_PlatformTarget_inObjectStreamOutObjectStream, opType, arg, inMemPtr, outMemPtr);

                ExceptionCheck(env, err);

                return LocalToGlobal(env, res);
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

            jobject JniContext::CacheOutOpQueryCursor(jobject obj, int type, long long memPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(
                    obj, jvm->GetMembers().m_PlatformTarget_inStreamOutObject, type, memPtr);

                ExceptionCheck(env, err);

                return LocalToGlobal(env, res);
            }

            jobject JniContext::CacheOutOpContinuousQuery(jobject obj, int type, long long memPtr, JniErrorInfo* err) {
                JNIEnv* env = Attach();

                jobject res = env->CallObjectMethod(
                    obj, jvm->GetMembers().m_PlatformTarget_inStreamOutObject, type, memPtr);

                ExceptionCheck(env, err);

                return LocalToGlobal(env, res);
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

            void JniContext::SetConsoleHandler(ConsoleWriteHandler consoleHandler) {
                if (!consoleHandler)
                    throw std::invalid_argument("consoleHandler can not be null");

                CONSOLE_LOCK.Enter();

                consoleWriteHandlers.push_back(consoleHandler);

                CONSOLE_LOCK.Leave();
            }

            int JniContext::RemoveConsoleHandler(ConsoleWriteHandler consoleHandler) {
                if (!consoleHandler)
                    throw std::invalid_argument("consoleHandler can not be null");

                CONSOLE_LOCK.Enter();

                int oldSize = static_cast<int>(consoleWriteHandlers.size());

                consoleWriteHandlers.erase(remove(consoleWriteHandlers.begin(), consoleWriteHandlers.end(),
                    consoleHandler), consoleWriteHandlers.end());

                int removedCnt = oldSize - static_cast<int>(consoleWriteHandlers.size());

                CONSOLE_LOCK.Leave();

                return removedCnt;
            }

            void JniContext::ThrowToJava(char* msg) {
                JNIEnv* env = Attach();

                env->ThrowNew(jvm->GetMembers().c_IgniteException, msg);
            }

            void JniContext::DestroyJvm() {
                jvm->GetJvm()->DestroyJavaVM();
            }

            /**
             * Attach thread to JVM.
             */
            JNIEnv* JniContext::Attach() {
                JNIEnv* env;

                jint attachRes = jvm->GetJvm()->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);

                if (attachRes == JNI_OK)
                    AttachHelper::OnThreadAttach();
                else {
                    if (hnds.error)
                        hnds.error(hnds.target, IGNITE_JNI_ERR_JVM_ATTACH, NULL, 0, NULL, 0, NULL, 0, NULL, 0);
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
                    jstring trace = static_cast<jstring>(env->CallStaticObjectMethod(jvm->GetJavaMembers().c_PlatformUtils, jvm->GetJavaMembers().m_PlatformUtils_getFullStackTrace, err));

                    env->DeleteLocalRef(cls);

                    int clsNameLen;
                    std::string clsName0 = JavaStringToCString(env, clsName, &clsNameLen);

                    int msgLen;
                    std::string msg0 = JavaStringToCString(env, msg, &msgLen);

                    int traceLen;
                    std::string trace0 = JavaStringToCString(env, trace, &traceLen);

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
                            hnds.error(hnds.target, IGNITE_JNI_ERR_GENERIC, clsName0.c_str(), clsNameLen, msg0.c_str(),
								msgLen, trace0.c_str(), traceLen, errBytesNative, errBytesLen);

                        env->ReleaseByteArrayElements(errData, errBytesNative, JNI_ABORT);
                    }
                    else
                    {
                        if (hnds.error)
                            hnds.error(hnds.target, IGNITE_JNI_ERR_GENERIC, clsName0.c_str(), clsNameLen, msg0.c_str(),
								msgLen, trace0.c_str(), traceLen, NULL, 0);
                    }

                    env->DeleteLocalRef(err);
                }
            }

            /**
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

            JNIEXPORT void JNICALL JniConsoleWrite(JNIEnv *env, jclass cls, jstring str, jboolean isErr) {
                CONSOLE_LOCK.Enter();

                if (consoleWriteHandlers.size() > 0) {
                    ConsoleWriteHandler consoleWrite = consoleWriteHandlers.at(0);

                    const char* strChars = env->GetStringUTFChars(str, 0);
                    const int strCharsLen = env->GetStringUTFLength(str);

                    consoleWrite(strChars, strCharsLen, isErr);

                    env->ReleaseStringUTFChars(str, strChars);
                }

                CONSOLE_LOCK.Leave();
            }

            JNIEXPORT void JNICALL JniLoggerLog(JNIEnv *env, jclass cls, jlong envPtr, jint level, jstring message, jstring category, jstring errorInfo, jlong memPtr) {
                int messageLen;
                char* messageChars = StringToChars(env, message, &messageLen);

                int categoryLen;
                char* categoryChars = StringToChars(env, category, &categoryLen);

                int errorInfoLen;
                char* errorInfoChars = StringToChars(env, errorInfo, &errorInfoLen);

                IGNITE_SAFE_PROC(env, envPtr, LoggerLogHandler, loggerLog, level, messageChars, messageLen, categoryChars, categoryLen, errorInfoChars, errorInfoLen, memPtr);

                if (messageChars)
                    delete[] messageChars;

                if (categoryChars)
                    delete[] categoryChars;

                if (errorInfoChars)
                    delete[] errorInfoChars;
            }

            JNIEXPORT jboolean JNICALL JniLoggerIsLevelEnabled(JNIEnv *env, jclass cls, jlong envPtr, jint level) {
                IGNITE_SAFE_FUNC(env, envPtr, LoggerIsLevelEnabledHandler, loggerIsLevelEnabled, level);
            }

            JNIEXPORT jlong JNICALL JniInLongOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint type, jlong val) {
                IGNITE_SAFE_FUNC(env, envPtr, InLongOutLongHandler, inLongOutLong, type, val);
            }

            JNIEXPORT jlong JNICALL JniInLongLongLongObjectOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint type, jlong val1, jlong val2, jlong val3, jobject arg) {
                IGNITE_SAFE_FUNC(env, envPtr, InLongLongLongObjectOutLongHandler, inLongLongLongObjectOutLong, type, val1, val2, val3, arg);
            }
        }
    }
}
