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

#ifndef _IGNITE_JNI_JAVA
#define _IGNITE_JNI_JAVA

#include <stdint.h>

#include <jni.h>

#include <ignite/common/common.h>

namespace ignite
{
    namespace jni
    {
        namespace java
        {
            /* Handlers for callbacks from Java. */
            typedef int64_t(JNICALL *CacheStoreCreateHandler)(void* target, int64_t memPtr);
            typedef int(JNICALL *CacheStoreInvokeHandler)(void* target, int64_t objPtr, int64_t memPtr);
            typedef void(JNICALL *CacheStoreDestroyHandler)(void* target, int64_t objPtr);
            typedef int64_t(JNICALL *CacheStoreSessionCreateHandler)(void* target, int64_t storePtr);

            typedef int64_t(JNICALL *CacheEntryFilterCreateHandler)(void* target, int64_t memPtr);
            typedef int(JNICALL *CacheEntryFilterApplyHandler)(void* target, int64_t ptr, int64_t memPtr);
            typedef void(JNICALL *CacheEntryFilterDestroyHandler)(void* target, int64_t ptr);

            typedef void(JNICALL *CacheInvokeHandler)(void* target, int64_t inMemPtr, int64_t outMemPtr);

            typedef void(JNICALL *ComputeTaskMapHandler)(void* target, int64_t taskPtr, int64_t inMemPtr, int64_t outMemPtr);
            typedef int(JNICALL *ComputeTaskJobResultHandler)(void* target, int64_t taskPtr, int64_t jobPtr, int64_t memPtr);
            typedef void(JNICALL *ComputeTaskReduceHandler)(void* target, int64_t taskPtr);
            typedef void(JNICALL *ComputeTaskCompleteHandler)(void* target, int64_t taskPtr, int64_t memPtr);
            typedef int(JNICALL *ComputeJobSerializeHandler)(void* target, int64_t jobPtr, int64_t memPtr);
            typedef int64_t(JNICALL *ComputeJobCreateHandler)(void* target, int64_t memPtr);
            typedef void(JNICALL *ComputeJobExecuteHandler)(void* target, int64_t jobPtr, int cancel, int64_t memPtr);
            typedef void(JNICALL *ComputeJobCancelHandler)(void* target, int64_t jobPtr);
            typedef void(JNICALL *ComputeJobDestroyHandler)(void* target, int64_t jobPtr);

            typedef void(JNICALL *ContinuousQueryListenerApplyHandler)(void* target, int64_t lsnrPtr, int64_t memPtr);
            typedef int64_t(JNICALL *ContinuousQueryFilterCreateHandler)(void* target, int64_t memPtr);
            typedef int(JNICALL *ContinuousQueryFilterApplyHandler)(void* target, int64_t filterPtr, int64_t memPtr);
            typedef void(JNICALL *ContinuousQueryFilterReleaseHandler)(void* target, int64_t filterPtr);

            typedef void(JNICALL *DataStreamerTopologyUpdateHandler)(void* target, int64_t ldrPtr, int64_t topVer, int topSize);
            typedef void(JNICALL *DataStreamerStreamReceiverInvokeHandler)(void* target, int64_t ptr, void* cache, int64_t memPtr, unsigned char keepPortable);

            typedef void(JNICALL *FutureByteResultHandler)(void* target, int64_t futAddr, int res);
            typedef void(JNICALL *FutureBoolResultHandler)(void* target, int64_t futAddr, int res);
            typedef void(JNICALL *FutureShortResultHandler)(void* target, int64_t futAddr, int res);
            typedef void(JNICALL *FutureCharResultHandler)(void* target, int64_t futAddr, int res);
            typedef void(JNICALL *FutureIntResultHandler)(void* target, int64_t futAddr, int res);
            typedef void(JNICALL *FutureFloatResultHandler)(void* target, int64_t futAddr, float res);
            typedef void(JNICALL *FutureLongResultHandler)(void* target, int64_t futAddr, int64_t res);
            typedef void(JNICALL *FutureDoubleResultHandler)(void* target, int64_t futAddr, double res);
            typedef void(JNICALL *FutureObjectResultHandler)(void* target, int64_t futAddr, int64_t memPtr);
            typedef void(JNICALL *FutureNullResultHandler)(void* target, int64_t futAddr);
            typedef void(JNICALL *FutureErrorHandler)(void* target, int64_t futAddr, int64_t memPtr);

            typedef void(JNICALL *LifecycleEventHandler)(void* target, int64_t ptr, int evt);

            typedef void(JNICALL *MemoryReallocateHandler)(void* target, int64_t memPtr, int cap);

            typedef int64_t(JNICALL *MessagingFilterCreateHandler)(void* target, int64_t memPtr);
            typedef int(JNICALL *MessagingFilterApplyHandler)(void* target, int64_t ptr, int64_t memPtr);
            typedef void(JNICALL *MessagingFilterDestroyHandler)(void* target, int64_t ptr);

            typedef int64_t(JNICALL *EventFilterCreateHandler)(void* target, int64_t memPtr);
            typedef int(JNICALL *EventFilterApplyHandler)(void* target, int64_t ptr, int64_t memPtr);
            typedef void(JNICALL *EventFilterDestroyHandler)(void* target, int64_t ptr);

            typedef int64_t(JNICALL *ServiceInitHandler)(void* target, int64_t memPtr);
            typedef void(JNICALL *ServiceExecuteHandler)(void* target, int64_t svcPtr, int64_t memPtr);
            typedef void(JNICALL *ServiceCancelHandler)(void* target, int64_t svcPtr, int64_t memPtr);
            typedef void(JNICALL *ServiceInvokeMethodHandler)(void* target, int64_t svcPtr, int64_t inMemPtr, int64_t outMemPtr);
            typedef int(JNICALL *ClusterNodeFilterApplyHandler)(void* target, int64_t memPtr);

            typedef int64_t(JNICALL *NodeInfoHandler)(void* target, int64_t memPtr);

            typedef void(JNICALL *OnStartHandler)(void* target, void* proc, int64_t memPtr);
            typedef void(JNICALL *OnStopHandler)(void* target);
            typedef void(JNICALL *ErrorHandler)(void* target, int errCode, const char* errClsChars, int errClsCharsLen, const char* errMsgChars, int errMsgCharsLen, const char* stackTraceChars, int stackTraceCharsLen, void* errData, int errDataLen);

            typedef int64_t(JNICALL *ExtensionCallbackInLongOutLongHandler)(void* target, int typ, int64_t arg1);
            typedef int64_t(JNICALL *ExtensionCallbackInLongLongOutLongHandler)(void* target, int typ, int64_t arg1, int64_t arg2);

            typedef void(JNICALL *OnClientDisconnectedHandler)(void* target);
            typedef void(JNICALL *OnClientReconnectedHandler)(void* target, unsigned char clusterRestarted);

            typedef int64_t(JNICALL *AffinityFunctionInitHandler)(void* target, int64_t memPtr, void* baseFunc);
            typedef int(JNICALL *AffinityFunctionPartitionHandler)(void* target, int64_t ptr, int64_t memPtr);
            typedef void(JNICALL *AffinityFunctionAssignPartitionsHandler)(void* target, int64_t ptr, int64_t inMemPtr, int64_t outMemPtr);
            typedef void(JNICALL *AffinityFunctionRemoveNodeHandler)(void* target, int64_t ptr, int64_t memPtr);
            typedef void(JNICALL *AffinityFunctionDestroyHandler)(void* target, int64_t ptr);

            typedef void(JNICALL *ConsoleWriteHandler)(const char* chars, int charsLen, unsigned char isErr);

            typedef void(JNICALL *LoggerLogHandler)(void* target, int level, const char* messageChars, int messageCharsLen, const char* categoryChars, int categoryCharsLen, const char* errorInfoChars, int errorInfoCharsLen, int64_t memPtr);
            typedef bool(JNICALL *LoggerIsLevelEnabledHandler)(void* target, int level);
            
            typedef int64_t(JNICALL *InLongOutLongHandler)(void* target, int type, int64_t val);
            typedef int64_t(JNICALL *InLongLongLongObjectOutLongHandler)(void* target, int type, int64_t val1, int64_t val2, int64_t val3, void* arg);

            /**
             * Is Java 9 or later is used.
             *
             * @return true if the Java 9 or later is in use.
             */
            bool IGNITE_IMPORT_EXPORT IsJava9OrLater();

            /**
             * JNI handlers holder.
             */
            struct JniHandlers {
                void* target;

                ErrorHandler error;

                LoggerLogHandler loggerLog;
                LoggerIsLevelEnabledHandler loggerIsLevelEnabled;

                InLongOutLongHandler inLongOutLong;
                InLongLongLongObjectOutLongHandler inLongLongLongObjectOutLong;
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

                jclass c_PlatformUtils;
                jmethodID m_PlatformUtils_getFullStackTrace;

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
                bool WriteErrorInfo(JNIEnv* env, char** errClsName, int* errClsNameLen, char** errMsg, int* errMsgLen,
                    char** stackTrace, int* stackTraceLen);
            };

            /**
             * JNI members.
             */
            struct JniMembers {
                jclass c_IgniteException;

                jclass c_PlatformIgnition;
                jmethodID m_PlatformIgnition_start;
                jmethodID m_PlatformIgnition_instance;
                jmethodID m_PlatformIgnition_environmentPointer;
                jmethodID m_PlatformIgnition_stop;
                jmethodID m_PlatformIgnition_stopAll;

                jclass c_PlatformTarget;
                jmethodID m_PlatformTarget_inLongOutLong;
                jmethodID m_PlatformTarget_inStreamOutLong;
                jmethodID m_PlatformTarget_inStreamOutObject;
                jmethodID m_PlatformTarget_outStream;
                jmethodID m_PlatformTarget_outObject;
                jmethodID m_PlatformTarget_inStreamAsync;
                jmethodID m_PlatformTarget_inStreamOutObjectAsync;
                jmethodID m_PlatformTarget_inStreamOutStream;
                jmethodID m_PlatformTarget_inObjectStreamOutObjectStream;

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
                static int Reallocate(int64_t memPtr, int cap);
                static void Detach();
                static void Release(jobject obj);
                static void SetConsoleHandler(ConsoleWriteHandler consoleHandler);
                static int RemoveConsoleHandler(ConsoleWriteHandler consoleHandler);

                void IgnitionStart(char* cfgPath, char* name, int factoryId, int64_t dataPtr);
                void IgnitionStart(char* cfgPath, char* name, int factoryId, int64_t dataPtr, JniErrorInfo* errInfo);
                int64_t IgnitionEnvironmentPointer(char* name);
                int64_t IgnitionEnvironmentPointer(char* name, JniErrorInfo* errInfo);
                bool IgnitionStop(char* name, bool cancel);
                bool IgnitionStop(char* name, bool cancel, JniErrorInfo* errInfo);
                void IgnitionStopAll(bool cancel);
                void IgnitionStopAll(bool cancel, JniErrorInfo* errInfo);
                
                int64_t TargetInLongOutLong(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);
                int64_t TargetInStreamOutLong(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);
                void TargetInStreamOutStream(jobject obj, int opType, int64_t inMemPtr, int64_t outMemPtr, JniErrorInfo* errInfo = NULL);
                jobject TargetInStreamOutObject(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);
                jobject TargetInObjectStreamOutObjectStream(jobject obj, int opType, void* arg, int64_t inMemPtr, int64_t outMemPtr, JniErrorInfo* errInfo = NULL);
                void TargetOutStream(jobject obj, int opType, int64_t memPtr, JniErrorInfo* errInfo = NULL);
                jobject TargetOutObject(jobject obj, int opType, JniErrorInfo* errInfo = NULL);
                void TargetInStreamAsync(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);
                jobject TargetInStreamOutObjectAsync(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);

                jobject CacheOutOpQueryCursor(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);
                jobject CacheOutOpContinuousQuery(jobject obj, int type, int64_t memPtr, JniErrorInfo* errInfo = NULL);

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
            };

            JNIEXPORT jlong JNICALL JniCacheStoreCreate(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr);
            JNIEXPORT jint JNICALL JniCacheStoreInvoke(JNIEnv *env, jclass cls, jlong envPtr, jlong objPtr, jlong memPtr);
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

            JNIEXPORT void JNICALL JniOnClientDisconnected(JNIEnv *env, jclass cls, jlong envPtr);
            JNIEXPORT void JNICALL JniOnClientReconnected(JNIEnv *env, jclass cls, jlong envPtr, jboolean clusterRestarted);

            JNIEXPORT jlong JNICALL JniAffinityFunctionInit(JNIEnv *env, jclass cls, jlong envPtr, jlong memPtr, jobject baseFunc);
            JNIEXPORT jint JNICALL JniAffinityFunctionPartition(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong memPtr);
            JNIEXPORT void JNICALL JniAffinityFunctionAssignPartitions(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong inMemPtr, jlong outMemPtr);
            JNIEXPORT void JNICALL JniAffinityFunctionRemoveNode(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr, jlong memPtr);
            JNIEXPORT void JNICALL JniAffinityFunctionDestroy(JNIEnv *env, jclass cls, jlong envPtr, jlong ptr);

            JNIEXPORT void JNICALL JniConsoleWrite(JNIEnv *env, jclass cls, jstring str, jboolean isErr);

            JNIEXPORT void JNICALL JniLoggerLog(JNIEnv *env, jclass cls, jlong envPtr, jint level, jstring message, jstring category, jstring errorInfo, jlong memPtr);
            JNIEXPORT jboolean JNICALL JniLoggerIsLevelEnabled(JNIEnv *env, jclass cls, jlong envPtr, jint level);

            JNIEXPORT jlong JNICALL JniInLongOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint type, jlong val);
            JNIEXPORT jlong JNICALL JniInLongLongLongObjectOutLong(JNIEnv *env, jclass cls, jlong envPtr, jint type, jlong val1, jlong val2, jlong val3, jobject arg);
        }
    }
}

#endif //_IGNITE_JNI_JAVA
