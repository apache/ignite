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

    long long IGNITE_CALL IgniteTargetInLongOutLong(gcj::JniContext* ctx, void* obj, int opType, long long val) {
        return ctx->TargetInLongOutLong(static_cast<jobject>(obj), opType, val);
    }

    bool IGNITE_CALL IgniteProcessorLoggerIsLevelEnabled(gcj::JniContext* ctx, void* obj, int level) {
        return ctx->ProcessorLoggerIsLevelEnabled(static_cast<jobject>(obj), level);
    }

    void IGNITE_CALL IgniteProcessorLoggerLog(gcj::JniContext* ctx, void* obj, int level, char* message, char* category, char* errorInfo) {
        ctx->ProcessorLoggerLog(static_cast<jobject>(obj), level, message, category, errorInfo);
    }

    void* IGNITE_CALL IgniteProcessorBinaryProcessor(gcj::JniContext* ctx, void* obj) {
        return ctx->ProcessorBinaryProcessor(static_cast<jobject>(obj));
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

    void* IGNITE_CALL IgniteTargetInObjectStreamOutObjectStream(gcj::JniContext* ctx, void* obj, int opType, void* arg, long long inMemPtr, long long outMemPtr) {
        return ctx->TargetInObjectStreamOutObjectStream(static_cast<jobject>(obj), opType, arg, inMemPtr, outMemPtr);
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

    void IGNITE_CALL IgniteSetConsoleHandler(gcj::ConsoleWriteHandler consoleHandler) {
        gcj::JniContext::SetConsoleHandler(consoleHandler);
    }

    void IGNITE_CALL IgniteRemoveConsoleHandler(gcj::ConsoleWriteHandler consoleHandler) {
        gcj::JniContext::RemoveConsoleHandler(consoleHandler);
    }
}