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
    int IGNITE_CALL IgniteReallocate(int64_t memPtr, int cap) {
        return gcj::JniContext::Reallocate(memPtr, cap);
    }

    void IGNITE_CALL IgniteIgnitionStart(gcj::JniContext* ctx, char* cfgPath, char* name, int factoryId, int64_t dataPtr) {
        ctx->IgnitionStart(cfgPath, name, factoryId, dataPtr);
    }

    int64_t IGNITE_CALL IgniteIgnitionEnvironmentPointer(gcj::JniContext* ctx, char* name) {
        return ctx->IgnitionEnvironmentPointer(name);
    }

	bool IGNITE_CALL IgniteIgnitionStop(gcj::JniContext* ctx, char* name, bool cancel) {
        return ctx->IgnitionStop(name, cancel);
    }

	void IGNITE_CALL IgniteIgnitionStopAll(gcj::JniContext* ctx, bool cancel) {
        return ctx->IgnitionStopAll(cancel);
    }

    int64_t IGNITE_CALL IgniteTargetInLongOutLong(gcj::JniContext* ctx, void* obj, int opType, int64_t val) {
        return ctx->TargetInLongOutLong(static_cast<jobject>(obj), opType, val);
    }

    int64_t IGNITE_CALL IgniteTargetInStreamOutLong(gcj::JniContext* ctx, void* obj, int opType, int64_t memPtr) {
        return ctx->TargetInStreamOutLong(static_cast<jobject>(obj), opType, memPtr);
    }

    void IGNITE_CALL IgniteTargetInStreamOutStream(gcj::JniContext* ctx, void* obj, int opType, int64_t inMemPtr, int64_t outMemPtr) {
        ctx->TargetInStreamOutStream(static_cast<jobject>(obj), opType, inMemPtr, outMemPtr);
    }

    void* IGNITE_CALL IgniteTargetInStreamOutObject(gcj::JniContext* ctx, void* obj, int opType, int64_t memPtr) {
        return ctx->TargetInStreamOutObject(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetInObjectStreamOutObjectStream(gcj::JniContext* ctx, void* obj, int opType, void* arg, int64_t inMemPtr, int64_t outMemPtr) {
        return ctx->TargetInObjectStreamOutObjectStream(static_cast<jobject>(obj), opType, arg, inMemPtr, outMemPtr);
    }

    void IGNITE_CALL IgniteTargetOutStream(gcj::JniContext* ctx, void* obj, int opType, int64_t memPtr) {
        ctx->TargetOutStream(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetOutObject(gcj::JniContext* ctx, void* obj, int opType) {
        return ctx->TargetOutObject(static_cast<jobject>(obj), opType);
    }

    void IGNITE_CALL IgniteTargetInStreamAsync(gcj::JniContext* ctx, void* obj, int opType, int64_t memPtr) {
        ctx->TargetInStreamAsync(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetInStreamOutObjectAsync(gcj::JniContext* ctx, void* obj, int opType, int64_t memPtr) {
        return ctx->TargetInStreamOutObjectAsync(static_cast<jobject>(obj), opType, memPtr);
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
